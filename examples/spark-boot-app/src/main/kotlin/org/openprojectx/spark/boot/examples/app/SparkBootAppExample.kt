package org.openprojectx.spark.boot.examples.app

import java.nio.file.Files
import org.apache.spark.sql.SaveMode
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.SparkBootContext
import org.openprojectx.spark.boot.dsl.kotlin.filterSql
import org.openprojectx.spark.boot.dsl.kotlin.runSparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.select
import org.openprojectx.spark.boot.dsl.kotlin.writeParquet

@SparkBoot
fun main(args: Array<String>) = runSparkBoot(args) {
    val input = Files.createTempDirectory("spark-boot-app-input")
    val output = Files.createTempDirectory("spark-boot-app-output")
    Files.delete(output)

    spark.createDataFrame(
        listOf(
            Order(id = "1", amount = 10.0, status = "PAID"),
            Order(id = "2", amount = 20.0, status = "CANCELLED")
        ),
        Order::class.java
    ).write().mode(SaveMode.Overwrite).parquet(input.toString())

    paidOrdersFlow(input.toString(), output.toString())
}

fun SparkBootContext.paidOrdersFlow(
    input: String,
    output: String
) = flow("paid-orders-app") {
    parquetSource("orders") {
        path = input
    }
        .filterSql("paid-only") {
            condition = "status = 'PAID'"
        }
        .select("select-columns") {
            columns = listOf("id", "amount", "status")
        }
        .writeParquet("sink") {
            path = output
            mode = SaveMode.Overwrite
        }
}

data class Order(
    val id: String = "",
    val amount: Double = 0.0,
    val status: String = ""
)
