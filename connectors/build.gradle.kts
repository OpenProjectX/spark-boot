plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform") version "0.1.42"
}

sparkPlatform {
    line.set("spark3-scala213")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":autoconfigure"))
    api(project(":core"))
    api(project(":runtime-spark"))
    implementation(libs.dagger)

    compileOnly("org.apache.spark:spark-sql_2.13")
    testImplementation("org.apache.spark:spark-sql_2.13")
}
