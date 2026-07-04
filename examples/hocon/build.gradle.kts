plugins {
    application
    kotlin("jvm") version "2.2.21"
    id("org.openprojectx.spark.platform") version "0.1.41"
    id("org.openprojectx.bigdata-test") version "0.1.34"
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

    implementation("org.openprojectx.spark.boot:cli:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
    implementation("org.apache.spark:spark-hive_2.13")
    implementation("org.apache.hadoop:hadoop-aws")
    implementation("org.apache.iceberg:iceberg-spark-runtime-4.0_2.13")
    implementation("org.apache.iceberg:iceberg-hive-metastore:1.10.0")
    implementation("org.apache.iceberg:iceberg-aws-bundle")
    implementation("com.typesafe:config:1.4.5")

    testImplementation(platform("org.testcontainers:testcontainers-bom:2.0.4"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("com.mysql:mysql-connector-j:9.5.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.5.6")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.2")
}

application {
    mainClass.set("org.openprojectx.spark.boot.cli.SparkBootCliKt")
}

bigDataTest {
    autoConfigureJavaExecTasks.set(true)
    autoConfigureTestTasks.set(true)
    config.add("classpath:bigdata-test.toml")
    extensionConfig.add("classpath:bigdata-extensions.toml")
    extensionRuntime {
        useShadedArtifact.set(false)
        includeSpark.set(true)
        includeHadoop.set(true)
        hadoopVersion.set("3.4.2")
    }
}

tasks.named<JavaExec>("run") {
    args("src/main/resources/paid-orders.conf")
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
    systemProperty("spark.boot.iceberg.warehouse", "s3a://spark-boot-hocon-example/iceberg-warehouse")
}
