package org.openprojectx.spark.boot.dsl.kotlin

import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.core.ExecutableFlow
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dagger.SparkBootComponent

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class SparkBoot(
    val name: String = ""
)

class SparkBootContext(
    val args: Array<String>,
    val component: SparkBootComponent
) {
    val spark: SparkSession
        get() = component.sparkSession()

    fun flow(name: String, block: SparkFlowBuilder.() -> Unit): ExecutableFlow {
        return sparkFlow(name, component, block)
    }

    fun run(flow: ExecutableFlow) {
        component.sparkRuntime().run(flow)
    }
}

fun runSparkBoot(
    args: Array<String> = emptyArray(),
    component: SparkBootComponent? = null,
    block: SparkBootContext.() -> ExecutableFlow
) {
    activateProfiles(args)
    val sparkBootComponent = component ?: DaggerSparkBootComponent.create()
    val context = SparkBootContext(args, sparkBootComponent)
    val spark = context.spark

    try {
        context.run(context.block())
    } finally {
        spark.stop()
    }
}

fun activateProfiles(args: Array<String>) {
    val profiles = args.profileArguments()
    if (profiles.isNotEmpty() && System.getProperty("spark.boot.profiles.active").isNullOrBlank()) {
        System.setProperty("spark.boot.profiles.active", profiles.joinToString(","))
    }
}

private fun Array<String>.profileArguments(): List<String> {
    return flatMapIndexed { index, arg ->
        when {
            arg == "--profile" || arg == "--spring.profiles.active" -> listOfNotNull(getOrNull(index + 1))
            arg.startsWith("--profile=") -> listOf(arg.substringAfter("="))
            arg.startsWith("--spring.profiles.active=") -> listOf(arg.substringAfter("="))
            else -> emptyList()
        }
    }
        .flatMap { value -> value.split(",", ";") }
        .map(String::trim)
        .filter(String::isNotBlank)
}
