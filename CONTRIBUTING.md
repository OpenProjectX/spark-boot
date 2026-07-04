# Contributing to Spark Boot

Spark Boot is organized around strict responsibility boundaries:

```text
Dagger = construction
Kotlin DSL = composition
HOCON DSL = declarative job description
Spark Runtime = execution
```

Keep changes aligned with those boundaries. Do not put Spark, Dagger, or HOCON dependencies into `core`.

## Development Setup

Requirements:

- Java 17
- Gradle wrapper from this repository
- Kotlin 2.2.x through the Gradle build
- Spark 4 dependencies managed by `org.openprojectx.spark.platform`

Use the shared Gradle cache when available:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew test --no-configuration-cache
```

## Project Layout

| Module | Contribution focus |
| --- | --- |
| `core` | Portable flow model and factory contracts only. |
| `runtime-spark` | Spark execution contracts, DAG validation, and runtime behavior. |
| `connectors` | Built-in Spark nodes and config factories. |
| `dagger` | Dagger component, modules, and factory map bindings. |
| `dsl-kotlin` | Programmatic Kotlin DSL and Spark-native escape hatches. |
| `dsl-hocon` | SeaTunnel-style HOCON parsing into `FlowDefinition`. |
| `cli` | HOCON file runner built on the public library modules. |
| `integration-tests` | Local Spark execution tests. |

The `examples` directory is intentionally a separate multi-module Gradle build.
It is excluded from the root project and should consume locally published Spark Boot snapshot artifacts from Maven local.

## Coding Guidelines

- Keep APIs small and explicit.
- Prefer Kotlin data classes for portable model types.
- Keep Spark classes out of `core`.
- Use Dagger map multibindings for config-driven node factories.
- Use explicit Spark-native names such as `transformDataFrame`; avoid ambiguous executor-style names like `map` or `filter`.
- Keep HOCON declarative. Do not add arbitrary scripting or JVM callback support to HOCON.
- Add validation errors that explain the invalid node, edge, or config key.

## Adding A Node

1. Add the runtime node in `connectors` or a new connector module.
2. Implement the relevant Spark contract:
   - `SparkSourceNode<O>`
   - `SparkTransformNode<I, O>`
   - `SparkSinkNode<I>`
3. Add a programmatic `NodeFactory` for Kotlin DSL use.
4. Add a `ConfigNodeFactory` for HOCON use when applicable.
5. Bind the config factory in a Dagger module with `@IntoMap` and `@StringKey`.
6. Add focused integration tests for runtime behavior.
7. Update `README.md` and `docs/user-guide.adoc` if the user-facing API changes.

## Testing

Run all tests:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew test --no-configuration-cache
```

Run only Spark integration tests:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew :integration-tests:test --no-configuration-cache
```

Before opening a change, make sure at least the affected modules compile and relevant tests pass.

## Local Publishing And Examples

The root project publishes Spark Boot modules as Maven artifacts. The standalone
`examples` build depends on those artifacts from Maven local, so publish the root
snapshot before compiling or running examples.

Publish all root modules to Maven local:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew publishToMavenLocal --no-configuration-cache
```

Compile the standalone examples build:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:compileKotlin --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :hocon:compileKotlin --no-configuration-cache
```

Run the examples:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :kotlin-dsl:run --no-configuration-cache
env GRADLE_USER_HOME=/data/.gradle ./gradlew -p examples :hocon:run --no-configuration-cache
```

The runnable Kotlin DSL example creates temporary Parquet input itself. If you
only need to verify that examples resolve the local snapshot artifacts, use
`compileKotlin`.

## Documentation

Update documentation with behavior changes:

- `README.md` for quick-start and high-level user-facing changes.
- `docs/user-guide.adoc` for detailed usage, configuration, extension points, or limitations.
- Example code in the standalone `examples` build when the recommended developer experience changes.

Use this product positioning consistently:

```text
Spark Boot Kotlin DSL:
  native programmatic API for developers

Spark Boot HOCON DSL:
  SeaTunnel-style declarative API for runtime/deployment configuration

Compatibility target:
  SeaTunnel-style configuration shape compatibility,
  not full Apache SeaTunnel runtime compatibility
```

## Pull Request Checklist

- The change keeps module dependency direction clean.
- Public API changes are documented.
- New runtime behavior has tests.
- HOCON parser changes include parser coverage or integration coverage.
- Spark-owned dependencies remain managed through Spark Platform.
- `./gradlew test --no-configuration-cache` passes, or the PR explains why it could not be run.
- If examples changed, `./gradlew publishToMavenLocal --no-configuration-cache` and the affected `./gradlew -p examples :<example>:compileKotlin --no-configuration-cache` task pass.

## Non-Goals For Current Contributions

Avoid adding these without a separate design discussion:

- full Apache SeaTunnel engine compatibility
- dynamic runtime plugin loading
- distributed job scheduling
- cluster submission services
- arbitrary scripting inside HOCON
- complex multi-input join runtime
- streaming checkpoint lifecycle management
- visual DAG editing
