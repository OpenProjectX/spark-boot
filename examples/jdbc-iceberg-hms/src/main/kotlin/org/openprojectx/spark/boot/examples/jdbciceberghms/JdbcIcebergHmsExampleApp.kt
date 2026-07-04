package org.openprojectx.spark.boot.examples.jdbciceberghms

import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.SparkBootContext

@SparkBoot
fun main(args: Array<String>) {
    MariaDbContainer().use { mariaDb ->
        mariaDb.start()
        seedOrders(mariaDb.jdbcUrl)
        assertSeededOrders(mariaDb.jdbcUrl)

        runSparkBootApplication(args) {
            val spark = spark
            assertJdbcSourceRows(spark, mariaDb.mysqlJdbcUrl)
            spark.sql("CREATE NAMESPACE IF NOT EXISTS hms.spark_boot_demo")

            component.runConfig(jdbcToIcebergConfig(mariaDb.mysqlJdbcUrl))
            assertIcebergRows(spark, "hms.spark_boot_demo.jdbc_orders")

            component.runKotlinJdbcToIcebergFlow(mariaDb.mysqlJdbcUrl)
            assertIcebergRows(spark, "hms.spark_boot_demo.jdbc_orders_kotlin")
        }
    }
}

private fun runSparkBootApplication(
    args: Array<String>,
    block: SparkBootContext.() -> Unit
) {
    val component = DaggerSparkBootComponent.create()
    val context = SparkBootContext(args, component)

    try {
        context.block()
    } finally {
        component.sparkSession().stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }
}
