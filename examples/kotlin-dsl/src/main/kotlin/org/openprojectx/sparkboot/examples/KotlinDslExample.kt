package org.openprojectx.sparkboot.examples

import java.nio.file.Files
import org.apache.spark.sql.SaveMode
import org.openprojectx.sparkboot.dagger.DaggerSparkBootComponent
import org.openprojectx.sparkboot.dsl.kotlin.filterSql
import org.openprojectx.sparkboot.dsl.kotlin.select
import org.openprojectx.sparkboot.dsl.kotlin.sparkFlow
import org.openprojectx.sparkboot.dsl.kotlin.writeParquet

fun main() {
    val component = DaggerSparkBootComponent.create()
    val spark = component.sparkSession()
    val input = Files.createTempDirectory("spark-boot-example-input")
    val output = Files.createTempDirectory("spark-boot-example-output")
    Files.delete(output)

    spark.createDataFrame(
        listOf(
            Order(id = "1", amount = 10.0, status = "PAID"),
            Order(id = "2", amount = 20.0, status = "CANCELLED")
        ),
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

    try {
        component.sparkRuntime().run(flow)
        spark.read().parquet(output.toString()).show(false)
    } finally {
        spark.stop()
    }
}

data class Order(
    val id: String = "",
    val amount: Double = 0.0,
    val status: String = ""
)
