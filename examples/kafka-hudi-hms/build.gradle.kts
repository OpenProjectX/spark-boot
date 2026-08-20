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
    addons.set(listOf("hadoopAws"))
    managedConfigurations.set(listOf("implementation", "testImplementation"))
}

dependencies {
    val sparkBootVersion = rootProject.extra["sparkBootVersion"] as String
    val hudiVersion = "1.2.0"

    implementation("org.openprojectx.spark.boot:dsl-hocon:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:dsl-kotlin:$sparkBootVersion")
    implementation("org.openprojectx.spark.boot:dagger:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
    implementation("org.apache.spark:spark-hive_2.13")
    implementation("org.apache.spark:spark-sql-kafka-0-10_2.13")
    implementation("org.apache.hadoop:hadoop-aws")
    implementation("org.apache.hudi:hudi-spark4.0-bundle_2.13:$hudiVersion")
    implementation("com.typesafe:config:1.4.5")
    implementation(platform("org.testcontainers:testcontainers-bom:2.0.4"))
    implementation("org.testcontainers:testcontainers")
    implementation("org.apache.kafka:kafka-clients:3.9.1")

    testImplementation("org.openprojectx.spark.boot:dsl-hocon:$sparkBootVersion")
    testImplementation("org.openprojectx.spark.boot:dsl-kotlin:$sparkBootVersion")
    testImplementation("org.openprojectx.spark.boot:dagger:$sparkBootVersion")
    testImplementation("org.apache.spark:spark-sql_2.13")
    testImplementation("org.apache.spark:spark-hive_2.13")
    testImplementation("org.apache.spark:spark-sql-kafka-0-10_2.13")
    testImplementation("org.apache.hadoop:hadoop-aws")
    testImplementation("org.apache.hudi:hudi-spark4.0-bundle_2.13:$hudiVersion")
    testImplementation("com.typesafe:config:1.4.5")

    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.2")
}

application {
    mainClass.set("org.openprojectx.spark.boot.examples.kafkahudihms.KafkaHudiHmsExampleAppKt")
}

bigDataTest {
    autoConfigureTestTasks.set(true)
    autoConfigureJavaExecTasks.set(true)
    config.add("classpath:bigdata-test.toml")
    extensionConfig.add("classpath:bigdata-extensions.toml")
    extensionRuntime {
        useShadedArtifact.set(false)
        includeSpark.set(true)
        includeHadoop.set(true)
        hadoopVersion.set("3.4.2")
    }
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
    systemProperty("spark.boot.hudi.warehouse", "s3a://spark-boot-kafka-hudi-hms/hudi-warehouse")
}

tasks.withType<JavaExec>().configureEach {
    minHeapSize = "1024m"
    maxHeapSize = "4096m"
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
    systemProperty("spark.boot.hudi.warehouse", "s3a://spark-boot-kafka-hudi-hms/hudi-warehouse")
}
