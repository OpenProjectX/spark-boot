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
    component: SparkBootComponent = DaggerSparkBootComponent.create(),
    block: SparkBootContext.() -> ExecutableFlow
) {
    val context = SparkBootContext(args, component)
    val spark = context.spark

    try {
        context.run(context.block())
    } finally {
        spark.stop()
    }
}
