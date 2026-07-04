package org.openprojectx.spark.boot.examples.app

import java.nio.file.Files
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.activateProfiles
import org.openprojectx.spark.boot.dsl.kotlin.runSparkBoot

@SparkBoot
fun main(args: Array<String>) {
    activateProfiles(args)
    runSparkBoot(args, DaggerSparkBootAppComponent.create()) {
        val output = Files.createTempDirectory("spark-boot-app-output")
        Files.delete(output)

        profiledPaidOrdersFlow(output.toString())
    }
}
