package org.openprojectx.sparkboot.examples.hocon

import java.nio.file.Files
import kotlin.io.path.writeText
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.openprojectx.sparkboot.cli.SparkBootCli

fun main() {
    val input = Files.createTempDirectory("spark-boot-hocon-example-input")
    val output = Files.createTempDirectory("spark-boot-hocon-example-output")
    val configFile = Files.createTempFile("spark-boot-hocon-example", ".conf")
    Files.delete(output)

    val spark = SparkSession.builder()
        .appName("spark-boot-hocon-example-data")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    try {
        spark.createDataFrame(
            listOf(
                Order(id = "1", amount = 10.0, status = "PAID"),
                Order(id = "2", amount = 20.0, status = "CANCELLED")
            ),
            Order::class.java
        ).write().mode(SaveMode.Overwrite).parquet(input.toString())
    } finally {
        spark.stop()
    }

    val templateUrl = object {}.javaClass.getResource("/paid-orders.conf")
        ?: error("Missing resource: paid-orders.conf")
    val template = templateUrl.openStream().bufferedReader().use { it.readText() }
    configFile.writeText(
        template
            .replace("__INPUT_PATH__", input.toString())
            .replace("__OUTPUT_PATH__", output.toString())
    )

    SparkBootCli.run(arrayOf(configFile.toString()))

    val resultSpark = SparkSession.builder()
        .appName("spark-boot-hocon-example-result")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    try {
        resultSpark.read().parquet(output.toString()).show(false)
    } finally {
        resultSpark.stop()
    }
}

data class Order(
    val id: String = "",
    val amount: Double = 0.0,
    val status: String = ""
)
