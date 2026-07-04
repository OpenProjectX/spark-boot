plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.41"
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("implementation", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    testImplementation(project(":core"))
    testImplementation(project(":runtime-spark"))
    testImplementation(project(":connectors"))
    testImplementation(project(":dagger"))
    testImplementation(project(":dsl-kotlin"))
    testImplementation(project(":dsl-hocon"))
    testImplementation(libs.junitJupiter)
    testImplementation(libs.typesafeConfig)
    testImplementation("org.apache.spark:spark-sql_2.13")
    testRuntimeOnly(libs.junitPlatformLauncher)
}

tasks.withType<Test>().configureEach {
    minHeapSize = "1024m"
    maxHeapSize = "4096m"
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
}
