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
| `integration-tests` | Local Spark integration tests. |

## Quick Start

Build and test:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew test --no-configuration-cache
```

Create and run a Kotlin DSL flow:

```kotlin
val component = DaggerSparkBootComponent.create()

val flow = sparkFlow("paid-orders", component) {
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

component.sparkRuntime().run(flow)
```

Publish the root project to Maven local before running examples:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew publishToMavenLocal --no-configuration-cache
```

Run the standalone examples build:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:run --no-configuration-cache
```

The `examples` directory is an independent multi-module Gradle build. It is not included in the root build and consumes `org.openprojectx.spark.boot:*:0.1.0-SNAPSHOT` artifacts from Maven local.
The current Kotlin DSL example is self-contained: it creates temporary Parquet input, runs the flow, prints the paid orders, and stops Spark.

## HOCON Example

```hocon
env {
  job.name = "paid-orders"
  job.mode = "BATCH"
}

source = [
  {
    plugin_name = "Parquet"
    path = "data/orders"
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
    path = "output/paid-orders"
    save_mode = "overwrite"
  }
]
```

See [docs/user-guide.adoc](docs/user-guide.adoc) for the detailed guide.
