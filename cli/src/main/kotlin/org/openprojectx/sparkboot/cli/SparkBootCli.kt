package org.openprojectx.sparkboot.cli

import com.typesafe.config.ConfigFactory
import java.io.File
import org.openprojectx.sparkboot.core.FlowAssembler
import org.openprojectx.sparkboot.dagger.DaggerSparkBootComponent
import org.openprojectx.sparkboot.dsl.hocon.SeaTunnelStyleConfigParser

fun main(args: Array<String>) {
    SparkBootCli.run(args)
}

object SparkBootCli {
    fun run(args: Array<String>) {
        val configFile = args.firstOrNull()
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
