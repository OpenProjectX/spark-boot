package org.openprojectx.spark.boot.examples.jdbciceberghms

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent

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
            configureSparkBootConnections(mariaDb.mysqlJdbcUrl)

            val component = DaggerSparkBootComponent.create()
            spark = component.sparkSession()
            assertJdbcSourceRows(component.sparkSession(), mariaDb.mysqlJdbcUrl)
            component.sparkSession().sql("CREATE NAMESPACE IF NOT EXISTS hms.spark_boot_demo")
            component.runConfig(jdbcToIcebergConfig())

            assertIcebergRows(component.sparkSession(), "hms.spark_boot_demo.jdbc_orders")
        }
    }

    @Test
    fun `runs kotlin dsl jdbc source into hms iceberg table`() {
        MariaDbContainer().use { mariaDb ->
            mariaDb.start()
            seedOrders(mariaDb.jdbcUrl)
            assertSeededOrders(mariaDb.jdbcUrl)
            configureSparkBootConnections(mariaDb.mysqlJdbcUrl)

            val component = DaggerSparkBootComponent.create()
            spark = component.sparkSession()
            assertJdbcSourceRows(component.sparkSession(), mariaDb.mysqlJdbcUrl)
            component.sparkSession().sql("CREATE NAMESPACE IF NOT EXISTS hms.spark_boot_demo")
            component.runKotlinJdbcToIcebergFlow()

            assertIcebergRows(component.sparkSession(), "hms.spark_boot_demo.jdbc_orders_kotlin")
        }
    }
}
