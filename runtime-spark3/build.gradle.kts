plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.42"
}

description = "Spark Boot runtime dependency carrier for Spark 3.5 with Scala 2.13."

sparkPlatform {
    line.set("spark3-scala213")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":runtime-spark"))

    api("org.apache.spark:spark-sql_2.13:3.5.8")
    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
}
