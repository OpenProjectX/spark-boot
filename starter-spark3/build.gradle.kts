plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.42"
}

description = "Spark Boot starter for Spark 3.5 with Scala 2.13."

sparkPlatform {
    line.set("spark3-scala213")
    variants.set(listOf("iceberg"))
    addons.set(listOf("hadoopAws", "icebergAws"))
    managedConfigurations.set(listOf("api", "implementation", "runtimeOnly", "compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":runtime-spark3"))
    api(project(":autoconfigure"))
    api(project(":core"))
    api(project(":dsl-hocon"))
    api(project(":dsl-kotlin"))
    api(project(":connectors"))
    api(project(":dagger"))
    api(project(":cli"))
}
