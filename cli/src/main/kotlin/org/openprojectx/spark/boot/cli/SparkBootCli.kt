package org.openprojectx.spark.boot.cli

import com.typesafe.config.ConfigFactory
import java.io.File
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dsl.hocon.SeaTunnelStyleConfigParser

fun main(args: Array<String>) {
    SparkBootCli.run(args)
}

object SparkBootCli {
    fun run(args: Array<String>) {
        val parsedArgs = SparkBootCliArgs.parse(args)
        parsedArgs.activateProfiles()

        val configFile = parsedArgs.configFile
            ?: error("Usage: spark-boot-cli <config-file>")

        val component = DaggerSparkBootComponent.create()
        val spark = component.sparkSession()

        try {
            val config = ConfigFactory.parseFile(File(configFile)).resolve()
            val definition = SeaTunnelStyleConfigParser().parse(config)
            val executableFlow = FlowAssembler(component.nodeFactoryRegistry()).assemble(definition)

            component.sparkRuntime().run(executableFlow)
        } finally {
            spark.stop()
        }
    }
}

private data class SparkBootCliArgs(
    val configFile: String?,
    val profiles: List<String>
) {
    fun activateProfiles() {
        if (profiles.isNotEmpty() && System.getProperty("spark.boot.profiles.active").isNullOrBlank()) {
            System.setProperty("spark.boot.profiles.active", profiles.joinToString(","))
        }
    }

    companion object {
        fun parse(args: Array<String>): SparkBootCliArgs {
            val profiles = mutableListOf<String>()
            var configFile: String? = null
            var index = 0

            while (index < args.size) {
                val arg = args[index]
                when {
                    arg == "--profile" || arg == "--spring.profiles.active" -> {
                        profiles += args.getOrNull(index + 1).orEmpty().splitProfiles()
                        index += 2
                    }
                    arg.startsWith("--profile=") -> {
                        profiles += arg.substringAfter("=").splitProfiles()
                        index += 1
                    }
                    arg.startsWith("--spring.profiles.active=") -> {
                        profiles += arg.substringAfter("=").splitProfiles()
                        index += 1
                    }
                    configFile == null -> {
                        configFile = arg
                        index += 1
                    }
                    else -> error("Unexpected argument: $arg")
                }
            }

            return SparkBootCliArgs(configFile, profiles)
        }
    }
}

private fun String.splitProfiles(): List<String> {
    return split(",", ";")
        .map(String::trim)
        .filter(String::isNotBlank)
}
