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
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :hocon:run --no-configuration-cache
```

The `:kotlin-dsl` example is self-contained. It creates temporary Parquet input,
runs the flow, prints the paid orders, and stops Spark.
The `:hocon` example is config-only. The `org.openprojectx.bigdata-test`
Gradle plugin starts LocalStack S3 and prepares the input Parquet data from
TOML config, then Gradle runs the Spark Boot CLI with `paid-orders.conf`.

## Adding More Examples

Add a new subdirectory with its own `build.gradle.kts`, then include it in `examples/settings.gradle.kts`:

```kotlin
include("new-example")
```

Example submodules should depend on published Spark Boot artifacts, not on root project modules.
