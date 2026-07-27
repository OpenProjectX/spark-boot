package org.openprojectx.spark.boot.examples.kafkahudihms

import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.SparkBootContext

@SparkBoot
fun main(args: Array<String>) {
    configureSparkBootEnvironment()
    val bootstrapServers = kafkaBootstrapServers()

    runSparkBootApplication(args) {
        component.prepareHudiDatabase()

        val configScenario = newScenario("config")
        seedKafkaOrders(bootstrapServers, configScenario.topic)
        component.runConfig(kafkaToHudiConfig(bootstrapServers, configScenario))
        assertHudiRows(spark, configScenario)
        assertHmsTable(spark, configScenario)

        val kotlinScenario = newScenario("kotlin")
        seedKafkaOrders(bootstrapServers, kotlinScenario.topic)
        component.runKotlinKafkaToHudiFlow(bootstrapServers, kotlinScenario)
        assertHudiRows(spark, kotlinScenario)
        assertHmsTable(spark, kotlinScenario)
    }
}

private fun runSparkBootApplication(
    args: Array<String>,
    block: SparkBootContext.() -> Unit
) {
    val component = DaggerSparkBootComponent.create()
    val context = SparkBootContext(args, component)

    try {
        context.block()
    } finally {
        component.sparkSession().stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }
}
