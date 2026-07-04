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

    implementation("org.openprojectx.spark.boot:core:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:runtime-spark:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:connectors:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:dagger:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:dsl-kotlin:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:dsl-hocon:$sparkBootVersion")
    implementation("com.typesafe:config:1.4.5")

    implementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.sparkboot.examples.KotlinDslExampleKt")
}
