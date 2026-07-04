plugins {
    application
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.41"
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("implementation", "testImplementation"))
}

dependencies {
    implementation(project(":core"))
    implementation(project(":runtime-spark"))
    implementation(project(":connectors"))
    implementation(project(":dagger"))
    implementation(project(":dsl-kotlin"))
    implementation(project(":dsl-hocon"))
    implementation(libs.typesafeConfig)

    implementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.sparkboot.examples.KotlinDslExampleKt")
}
