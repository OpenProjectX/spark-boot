plugins {
    application
    kotlin("jvm") version "2.2.21"
    id("org.openprojectx.spark.platform") version "0.1.41"
}

kotlin {
    jvmToolchain(17)
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("implementation"))
}

dependencies {
    val sparkBootVersion = rootProject.extra["sparkBootVersion"] as String

    implementation("org.openprojectx.spark.boot:dsl-kotlin:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.spark.boot.examples.app.SparkBootAppExampleKt")
}
