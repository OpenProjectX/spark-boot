package org.openprojectx.spark.boot.examples.app

import java.nio.file.Files
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.runSparkBoot

@SparkBoot
fun main(args: Array<String>) = runSparkBoot(args, DaggerSparkBootAppComponent.create()) {
    val output = Files.createTempDirectory("spark-boot-app-output")
    Files.delete(output)

    paidOrdersFlow(
        listOf(
            Order(id = "1", amount = 10.0, status = "PAID"),
            Order(id = "2", amount = 20.0, status = "CANCELLED")
        ),
        output.toString()
    )
}
