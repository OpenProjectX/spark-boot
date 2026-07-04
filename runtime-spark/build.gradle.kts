plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.41"
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":core"))

    compileOnly("org.apache.spark:spark-sql_2.13")
    testImplementation("org.apache.spark:spark-sql_2.13")
}
