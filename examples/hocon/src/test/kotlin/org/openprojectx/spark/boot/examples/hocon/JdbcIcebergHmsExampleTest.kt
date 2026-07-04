package org.openprojectx.spark.boot.examples.hocon

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import java.time.Duration
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dsl.hocon.SeaTunnelStyleConfigParser
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class JdbcIcebergHmsExampleTest {
    private var spark: SparkSession? = null

    @AfterEach
    fun stopSpark() {
        spark?.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    @Test
    fun `runs jdbc source into hms iceberg table`() {
        MariaDbContainer().use { mariaDb ->
            mariaDb.start()
            seedOrders(mariaDb.jdbcUrl)
            assertSeededOrders(mariaDb.jdbcUrl)

            val component = DaggerSparkBootComponent.create()
            spark = component.sparkSession()
            assertJdbcSourceRows(component.sparkSession(), mariaDb.mysqlJdbcUrl)
            component.sparkSession().sql("CREATE NAMESPACE IF NOT EXISTS hms.spark_boot_demo")
            component.runConfig(jdbcToIcebergConfig(mariaDb.mysqlJdbcUrl))

            val rows = component.sparkSession()
                .sql("SELECT id, amount, status FROM hms.spark_boot_demo.jdbc_orders ORDER BY id")
                .collectAsList()

            val values = rows.map { row ->
                Triple(row.getString(0), row.getString(1), row.getString(2))
            }

            assertTrue(values.any { it.first == "1" && it.third == "PAID" }, "Expected paid order in $values")
            assertTrue(values.any { it.first == "2" && it.third == "CANCELLED" }, "Expected cancelled order in $values")
        }
    }

    private fun seedOrders(jdbcUrl: String) {
        DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeUpdate("DROP TABLE IF EXISTS jdbc_orders")
                statement.executeUpdate(
                    """
                    CREATE TABLE jdbc_orders (
                        id VARCHAR(16) PRIMARY KEY,
                        amount VARCHAR(16) NOT NULL,
                        status VARCHAR(32) NOT NULL
                    )
                    """.trimIndent()
                )
                statement.executeUpdate(
                    """
                    INSERT INTO jdbc_orders (id, amount, status) VALUES
                    ('1', '10.00', 'PAID'),
                    ('2', '20.00', 'CANCELLED')
                    """.trimIndent()
                )
            }
        }
    }

    private fun assertSeededOrders(jdbcUrl: String) {
        DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT id, amount, status FROM jdbc_orders ORDER BY id").use { resultSet ->
                    val values = buildList {
                        while (resultSet.next()) {
                            add(
                                Triple(
                                    resultSet.getString("id"),
                                    resultSet.getString("amount"),
                                    resultSet.getString("status")
                                )
                            )
                        }
                    }

                    assertTrue(values.any { it.first == "1" && it.third == "PAID" }, "Expected seeded paid order in $values")
                    assertTrue(
                        values.any { it.first == "2" && it.third == "CANCELLED" },
                        "Expected seeded cancelled order in $values"
                    )
                }
            }
        }
    }

    private fun assertJdbcSourceRows(spark: SparkSession, jdbcUrl: String) {
        val values = spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", "jdbc_orders")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
            .select("id", "amount", "status")
            .collectAsList()
            .map { row -> Triple(row.getString(0), row.getString(1), row.getString(2)) }

        assertTrue(values.any { it.first == "1" && it.third == "PAID" }, "Expected JDBC paid order in $values")
        assertTrue(values.any { it.first == "2" && it.third == "CANCELLED" }, "Expected JDBC cancelled order in $values")
    }

    private fun SparkBootComponent.runConfig(configText: String) {
        val definition = SeaTunnelStyleConfigParser().parse(ConfigFactory.parseString(configText))
        val executableFlow = FlowAssembler(nodeFactoryRegistry()).assemble(definition)
        sparkRuntime().run(executableFlow)
    }

    private fun jdbcToIcebergConfig(jdbcUrl: String): String =
        """
        env {
          job.name = "jdbc-to-iceberg-hms"
        }

        source = [
          {
            plugin_name = "Jdbc"
            plugin_output = "jdbc_orders"
            url = "$jdbcUrl"
            table = "jdbc_orders"
            user = "$DB_USER"
            password = "$DB_PASSWORD"
            driver = "com.mysql.cj.jdbc.Driver"
          }
        ]

        sink = [
          {
            plugin_name = "Iceberg"
            plugin_input = "jdbc_orders"
            table = "hms.spark_boot_demo.jdbc_orders"
            save_mode = "overwrite"
          }
        ]
        """.trimIndent()

    private class MariaDbContainer : GenericContainer<MariaDbContainer>(
        DockerImageName.parse("ghcr.io/openprojectx/dockerhub/library/mariadb:10.6.27-jammy")
    ) {
        val jdbcUrl: String
            get() = "jdbc:mariadb://$host:${getMappedPort(MARIADB_PORT)}/$DB_NAME"
        val mysqlJdbcUrl: String
            get() = "jdbc:mysql://$host:${getMappedPort(MARIADB_PORT)}/$DB_NAME?useSSL=false&allowPublicKeyRetrieval=true"

        init {
            withExposedPorts(MARIADB_PORT)
            withEnv("MARIADB_DATABASE", DB_NAME)
            withEnv("MARIADB_USER", DB_USER)
            withEnv("MARIADB_PASSWORD", DB_PASSWORD)
            withEnv("MARIADB_ROOT_PASSWORD", "root")
            waitingFor(
                Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(2))
            )
        }
    }

    private companion object {
        const val DB_NAME = "spark_boot"
        const val DB_USER = "spark"
        const val DB_PASSWORD = "spark"
        const val MARIADB_PORT = 3306
    }
}
