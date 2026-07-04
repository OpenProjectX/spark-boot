package org.openprojectx.spark.boot.examples.jdbciceberghms

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dsl.hocon.SeaTunnelStyleConfigParser
import org.openprojectx.spark.boot.dsl.kotlin.sparkFlow
import org.openprojectx.spark.boot.dsl.kotlin.writeIceberg

fun seedOrders(jdbcUrl: String) {
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

fun assertSeededOrders(jdbcUrl: String) {
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

                check(values.any { it.first == "1" && it.third == "PAID" }) {
                    "Expected seeded paid order in $values"
                }
                check(values.any { it.first == "2" && it.third == "CANCELLED" }) {
                    "Expected seeded cancelled order in $values"
                }
            }
        }
    }
}

fun assertJdbcSourceRows(spark: SparkSession, jdbcUrl: String) {
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

    check(values.any { it.first == "1" && it.third == "PAID" }) {
        "Expected JDBC paid order in $values"
    }
    check(values.any { it.first == "2" && it.third == "CANCELLED" }) {
        "Expected JDBC cancelled order in $values"
    }
}

fun assertIcebergRows(spark: SparkSession, table: String) {
    val values = spark
        .sql("SELECT id, amount, status FROM $table ORDER BY id")
        .collectAsList()
        .map { row -> Triple(row.getString(0), row.getString(1), row.getString(2)) }

    check(values.any { it.first == "1" && it.third == "PAID" }) {
        "Expected paid order in $values"
    }
    check(values.any { it.first == "2" && it.third == "CANCELLED" }) {
        "Expected cancelled order in $values"
    }
}

fun configureSparkBootConnections(jdbcUrl: String) {
    System.setProperty("spark.boot.jdbc.connections.orders.url", jdbcUrl)

    System.getProperty("hive.metastore.uris")?.let { metastoreUris ->
        System.setProperty("spark.boot.hms.uri", metastoreUris)
    }
    System.getProperty("spark.boot.iceberg.warehouse")?.let { warehouse ->
        System.setProperty("spark.boot.hms.warehouse", warehouse)
    }
}

fun SparkBootComponent.runConfig(configText: String) {
    val definition = SeaTunnelStyleConfigParser().parse(ConfigFactory.parseString(configText))
    val executableFlow = FlowAssembler(nodeFactoryRegistry()).assemble(definition)
    sparkRuntime().run(executableFlow)
}

fun SparkBootComponent.runKotlinJdbcToIcebergFlow() {
    val flow = sparkFlow("jdbc-to-iceberg-hms-kotlin", this) {
        jdbcSource("jdbc_orders") {
            connection = "orders"
            table = "jdbc_orders"
        }.writeIceberg("sink") {
            catalog = "hms"
            table = "spark_boot_demo.jdbc_orders_kotlin"
            mode = SaveMode.Overwrite
        }
    }

    sparkRuntime().run(flow)
}

fun jdbcToIcebergConfig(): String =
    """
    env {
      job.name = "jdbc-to-iceberg-hms"
    }

    source = [
      {
        plugin_name = "Jdbc"
        plugin_output = "jdbc_orders"
        connection = "orders"
        table = "jdbc_orders"
      }
    ]

    sink = [
      {
        plugin_name = "Iceberg"
        plugin_input = "jdbc_orders"
        catalog = "hms"
        table = "spark_boot_demo.jdbc_orders"
        save_mode = "overwrite"
      }
    ]
    """.trimIndent()
