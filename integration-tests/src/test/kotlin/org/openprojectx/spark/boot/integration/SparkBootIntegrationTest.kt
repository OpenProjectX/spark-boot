package org.openprojectx.spark.boot.integration

import com.typesafe.config.ConfigFactory
import java.nio.file.Files
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dsl.hocon.SeaTunnelStyleConfigParser
import org.openprojectx.spark.boot.dsl.kotlin.filterSql
import org.openprojectx.spark.boot.dsl.kotlin.select
import org.openprojectx.spark.boot.dsl.kotlin.sparkFlow
import org.openprojectx.spark.boot.dsl.kotlin.writeParquet

class SparkBootIntegrationTest {
    @Test
    fun `runs Kotlin DSL parquet flow`() {
        val input = Files.createTempDirectory("spark-boot-input")
        val output = Files.createTempDirectory("spark-boot-output")
        Files.delete(output)

        component.sparkSession().createDataFrame(
            listOf(Order("1", 10.0, "PAID"), Order("2", 20.0, "CANCELLED")),
            Order::class.java
        ).write().mode(SaveMode.Overwrite).parquet(input.toString())

        val flow = sparkFlow("paid-orders", component) {
            parquetSource("orders") {
                path = input.toString()
            }
                .filterSql("paid-only") {
                    condition = "status = 'PAID'"
                }
                .select("select-columns") {
                    columns = listOf("id", "amount", "status")
                }
                .writeParquet("sink") {
                    path = output.toString()
                    mode = SaveMode.Overwrite
                }
        }

        component.sparkRuntime().run(flow)

        val rows = component.sparkSession().read().parquet(output.toString()).collectAsList()
        assertEquals(1, rows.size)
        assertEquals("1", rows.single().getAs<String>("id"))
    }

    @Test
    fun `parses and runs SeaTunnel style HOCON flow`() {
        val input = Files.createTempDirectory("spark-boot-hocon-input")
        val output = Files.createTempDirectory("spark-boot-hocon-output")
        Files.delete(output)

        component.sparkSession().createDataFrame(
            listOf(Order("1", 10.0, "PAID"), Order("2", 20.0, "CANCELLED")),
            Order::class.java
        ).write().mode(SaveMode.Overwrite).parquet(input.toString())

        val config = ConfigFactory.parseString(
            """
            env {
              job.name = "paid-orders"
              job.mode = "BATCH"
            }

            source = [
              {
                plugin_name = "Parquet"
                path = "$input"
                plugin_output = "orders"
              }
            ]

            transform = [
              {
                plugin_name = "Sql"
                plugin_input = "orders"
                plugin_output = "paid_orders"
                query = "select id, amount, status from orders where status = 'PAID'"
              }
            ]

            sink = [
              {
                plugin_name = "Parquet"
                plugin_input = "paid_orders"
                path = "$output"
                save_mode = "overwrite"
              }
            ]
            """.trimIndent()
        ).resolve()

        val definition = SeaTunnelStyleConfigParser().parse(config)
        val executableFlow = FlowAssembler(component.nodeFactoryRegistry()).assemble(definition)

        component.sparkRuntime().run(executableFlow)

        val rows = component.sparkSession().read().parquet(output.toString()).collectAsList()
        assertEquals(1, rows.size)
        assertEquals("1", rows.single().getAs<String>("id"))
    }

    data class Order(
        val id: String = "",
        val amount: Double = 0.0,
        val status: String = ""
    )

    companion object {
        private val component = DaggerSparkBootComponent.create()

        @JvmStatic
        @AfterAll
        fun stopSpark() {
            component.sparkSession().stop()
        }
    }
}
