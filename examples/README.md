# Spark Boot Examples

This directory is an independent multi-module Gradle build. It is intentionally excluded from the root `spark-boot` project.

Examples consume Spark Boot artifacts from Maven local:

```text
org.openprojectx.spark.boot:*:0.1.0-SNAPSHOT
```

## Publish Spark Boot Locally

From the repository root:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew publishToMavenLocal --no-configuration-cache
```

## Run Examples

From the repository root:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples runAll --no-configuration-cache
```

Run individual app examples:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :spark-boot-app:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :hocon:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :jdbc-iceberg-hms:run --no-configuration-cache
```

The `:kotlin-dsl` example is self-contained. It creates temporary Parquet input,
runs the flow, prints the paid orders, and stops Spark.
The `:spark-boot-app` example shows the Spring Boot-style entry point with
`@SparkBoot` and `runSparkBoot`; it also contributes a custom node factory
through an app Dagger module and creates that node from the DSL with
`node<InMemoryOrdersSourceNode>("orders", "InMemoryOrdersSource")`.
It is an application example, not a JUnit test.
The `:hocon` example is config-only. The `org.openprojectx.bigdata-test`
Gradle plugin starts LocalStack S3 and prepares the input Parquet data from
TOML config, then Gradle runs the Spark Boot CLI with `paid-orders.conf`.

## Run JDBC Iceberg HMS

The `:jdbc-iceberg-hms` module is both a runnable app and a JUnit-backed
integration example. It starts LocalStack S3 and Hive Metastore through
`org.openprojectx.bigdata-test`; the app itself depends on Testcontainers and
starts `ghcr.io/openprojectx/dockerhub/library/mariadb:10.6.27-jammy` as the
JDBC source. It publishes the dynamic container endpoints into Spark Boot
starter-style config, then runs equivalent HOCON and Kotlin DSL flows from
`connection = "orders"` into `catalog = "hms"` HMS-backed Iceberg tables.

Run the app:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :jdbc-iceberg-hms:run --no-configuration-cache
```

Run the tests:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :jdbc-iceberg-hms:test --no-configuration-cache
```

## Adding More Examples

Add a new subdirectory with its own `build.gradle.kts`, then include it in `examples/settings.gradle.kts`:

```kotlin
include("new-example")
```

Example submodules should depend on published Spark Boot artifacts, not on root project modules.
