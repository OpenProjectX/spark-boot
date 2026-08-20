package org.openprojectx.spark.boot.examples.multihmscatalogs

import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot

@SparkBoot
fun main() {
    configureMultiHmsCatalogs()
    val component = DaggerSparkBootComponent.create()
    try {
        runMultiHmsCatalogsExample(component.sparkSession())
    } finally {
        component.sparkSession().stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }
}
