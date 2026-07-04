plugins {
    application
    id("buildsrc.convention.kotlin-jvm")
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
    api(project(":dagger"))
    api(project(":dsl-hocon"))
    implementation(libs.typesafeConfig)

    compileOnly("org.apache.spark:spark-sql_2.13")
    testImplementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.spark.boot.cli.SparkBootCliKt")
}
