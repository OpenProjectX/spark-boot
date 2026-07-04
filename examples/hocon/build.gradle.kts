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
    managedConfigurations.set(listOf("implementation", "testImplementation"))
}

dependencies {
    val sparkBootVersion = rootProject.extra["sparkBootVersion"] as String

    implementation("org.openprojectx.spark.boot:cli:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.sparkboot.examples.hocon.HoconExampleKt")
}
