package org.openprojectx.spark.boot.examples.multihmscatalogs

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent

class MultiHmsCatalogsExampleTest {
    private var spark: SparkSession? = null

    @AfterEach
    fun stopSpark() {
        spark?.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    @Test
    fun `runs iceberg and hive catalogs against two hms instances`() {
        configureMultiHmsCatalogs()
        val component = DaggerSparkBootComponent.create()
        spark = component.sparkSession()

        runMultiHmsCatalogsExample(component.sparkSession())
    }
}
