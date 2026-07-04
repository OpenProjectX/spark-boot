plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.jetbrains.kotlin.kapt")
    id("org.openprojectx.spark.platform") version "0.1.41"
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":core"))
    api(project(":runtime-spark"))
    api(project(":connectors"))
    api(libs.dagger)

    compileOnly("org.apache.spark:spark-sql_2.13")
    kapt(libs.daggerCompiler)
    testImplementation("org.apache.spark:spark-sql_2.13")
}
