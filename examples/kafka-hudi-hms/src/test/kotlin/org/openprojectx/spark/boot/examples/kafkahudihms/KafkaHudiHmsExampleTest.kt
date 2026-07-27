package org.openprojectx.spark.boot.examples.kafkahudihms

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent

class KafkaHudiHmsExampleTest {
    private var spark: SparkSession? = null

    @AfterEach
    fun stopSpark() {
        spark?.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    @Test
    fun `runs kafka source into hms synced hudi table from config`() {
        configureSparkBootEnvironment()
        val bootstrapServers = kafkaBootstrapServers()
        val scenario = newScenario("config-test")
        seedKafkaOrders(bootstrapServers, scenario.topic)

        val component = DaggerSparkBootComponent.create()
        spark = component.sparkSession()
        component.prepareHudiDatabase()
        component.runConfig(kafkaToHudiConfig(bootstrapServers, scenario))

        assertHudiRows(component.sparkSession(), scenario)
        assertHmsTable(component.sparkSession(), scenario)
    }

    @Test
    fun `runs kafka source into hms synced hudi table from kotlin dsl`() {
        configureSparkBootEnvironment()
        val bootstrapServers = kafkaBootstrapServers()
        val scenario = newScenario("kotlin-test")
        seedKafkaOrders(bootstrapServers, scenario.topic)

        val component = DaggerSparkBootComponent.create()
        spark = component.sparkSession()
        component.prepareHudiDatabase()
        component.runKotlinKafkaToHudiFlow(bootstrapServers, scenario)

        assertHudiRows(component.sparkSession(), scenario)
        assertHmsTable(component.sparkSession(), scenario)
    }
}
