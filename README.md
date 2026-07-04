# Spark Boot

Spark Boot is a JVM/Kotlin framework for defining, assembling, and running Spark pipelines with a clean separation between construction, composition, declarative configuration, and Spark execution.

It provides:

- a Kotlin DSL for developer-first pipeline authoring
- a SeaTunnel-style HOCON DSL for deployment/runtime configuration
- Dagger-based compile-time construction and factory registration
- a portable Flow / Node / Edge model
- a Spark 4 runtime backed by `org.openprojectx.spark.platform`
- built-in Parquet, SQL filter/select/transform, and JDBC sink nodes

Compatibility target:

```text
Spark Boot HOCON DSL supports SeaTunnel-style configuration shape compatibility,
not full Apache SeaTunnel runtime compatibility.
```

## Modules

| Module | Purpose |
| --- | --- |
| `core` | Flow model, node definitions, factory contracts, and assembler. |
| `runtime-spark` | Spark execution context, Spark node contracts, DAG validation, and runtime execution. |
| `connectors` | Built-in Spark nodes and config factories. |
| `dagger` | Dagger component, modules, and factory registry wiring. |
| `dsl-kotlin` | Kotlin DSL and fluent pipeline chaining. |
| `dsl-hocon` | SeaTunnel-style HOCON parser. |
| `cli` | HOCON file runner for users who want to provide only config. |
| `integration-tests` | Local Spark integration tests. |

## Quick Start

Build and test:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew test --no-configuration-cache
```

Create and run a Kotlin DSL flow:

```kotlin
@SparkBoot
fun main(args: Array<String>) = runSparkBoot(args) {
    flow("paid-orders") {
        parquetSource("orders") {
            path = "data/orders"
        }
            .filterSql("paid-only") {
                condition = "status = 'PAID'"
            }
            .select("select-columns") {
                columns = listOf("id", "amount", "status")
            }
            .writeParquet("sink") {
                path = "output/paid-orders"
                mode = SaveMode.Overwrite
            }
    }
}
```

Publish the root project to Maven local before running examples:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew publishToMavenLocal --no-configuration-cache
```

Run the standalone examples build:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples runAll --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :spark-boot-app:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :hocon:run --no-configuration-cache
```

The `examples` directory is an independent multi-module Gradle build. It is not included in the root build and consumes `org.openprojectx.spark.boot:*:0.1.0-SNAPSHOT` artifacts from Maven local.
The Kotlin DSL examples create temporary Parquet input in code. The `:spark-boot-app` example shows the `@SparkBoot` application entry point. The HOCON example is config-only: `org.openprojectx.bigdata-test` starts LocalStack S3 and prepares the Parquet input from TOML before the Spark Boot CLI runs `paid-orders.conf`.

## CLI

Applications can depend on the CLI module and provide only a HOCON config file:

```bash
java -cp "<app-and-dependencies>" org.openprojectx.spark.boot.cli.SparkBootCliKt paid-orders.conf
```

The CLI parses the config, assembles the flow with Dagger-backed built-in factories, and runs it with Spark.

## HOCON Example

```hocon
env {
  job.name = "paid-orders"
  job.mode = "BATCH"
}

source = [
  {
    plugin_name = "Parquet"
    path = "s3a://spark-boot-hocon-example/input/orders"
    plugin_output = "orders"
  }
]

transform = [
  {
    plugin_name = "Sql"
    plugin_input = "orders"
    plugin_output = "paid_orders"
    query = "select id, amount, status from orders where status = 'PAID'"
  }
]

sink = [
  {
    plugin_name = "Parquet"
    plugin_input = "paid_orders"
    path = "s3a://spark-boot-hocon-example/output/paid-orders"
    save_mode = "overwrite"
  }
]
```

See [docs/user-guide.adoc](docs/user-guide.adoc) for the detailed guide.
