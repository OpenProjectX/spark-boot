plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.41"
}

description = "Spark Boot starter for Spark 4 with Scala 2.13."

sparkPlatform {
    line.set("spark4")
    variants.set(listOf("iceberg", "hudi"))
    addons.set(listOf("hadoopAws", "icebergAws"))
    managedConfigurations.set(listOf("api", "implementation", "runtimeOnly", "compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":runtime-spark4"))
    api(project(":autoconfigure"))
    api(project(":core"))
    api(project(":dsl-hocon"))
    api(project(":dsl-kotlin"))
    api(project(":connectors"))
    api(project(":dagger"))
    api(project(":cli"))
}
