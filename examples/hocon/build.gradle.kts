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
    addons.set(listOf("hadoopAws"))
    managedConfigurations.set(listOf("implementation"))
}

dependencies {
    val sparkBootVersion = rootProject.extra["sparkBootVersion"] as String

    implementation("org.openprojectx.spark.boot:cli:$sparkBootVersion")
    implementation("org.apache.spark:spark-sql_2.13")
    implementation("org.apache.hadoop:hadoop-aws")
}

application {
    mainClass.set("org.openprojectx.spark.boot.cli.SparkBootCliKt")
}

bigDataTest {
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

tasks.named<JavaExec>("run") {
    args("src/main/resources/paid-orders.conf")
}
