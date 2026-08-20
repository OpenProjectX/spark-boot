plugins {
    application
    kotlin("jvm") version "2.2.21"
    id("org.openprojectx.spark.platform") version "0.1.42"
    id("org.openprojectx.bigdata-test") version "0.1.42"
}

kotlin {
    jvmToolchain(17)
}

sparkPlatform {
    line.set("spark4")
    variants.set(listOf("iceberg"))
    addons.set(listOf("hadoopAws", "icebergAws"))
    managedConfigurations.set(listOf("implementation", "testImplementation"))
}

dependencies {
    val sparkBootVersion = rootProject.extra["sparkBootVersion"] as String

    implementation("org.openprojectx.spark.boot:starter-spark4:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
    implementation("org.apache.spark:spark-hive_2.13")
    implementation("org.apache.hadoop:hadoop-aws")
    implementation("org.apache.iceberg:iceberg-spark-runtime-4.0_2.13")
    implementation("org.apache.iceberg:iceberg-hive-metastore:1.10.0")
    implementation("org.apache.iceberg:iceberg-aws-bundle")
    implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_2.13:1.11.0")

    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.2")
}

application {
    mainClass.set("org.openprojectx.spark.boot.examples.multihmscatalogs.MultiHmsCatalogsExampleAppKt")
}

bigDataTest {
    autoConfigureTestTasks.set(true)
    autoConfigureJavaExecTasks.set(true)
    config.add("classpath:bigdata-test.toml")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    minHeapSize = "1024m"
    maxHeapSize = "4096m"
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
}

tasks.withType<JavaExec>().configureEach {
    minHeapSize = "1024m"
    maxHeapSize = "4096m"
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
}
