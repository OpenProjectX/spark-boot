// ===========================================================================
// Parameterised job templates: the abstraction above the flow IR.
//
// A `JobTemplate` is a pure function `Config -> FlowDefinition`. Its
// `JobDescriptor` describes the function's INPUT DOMAIN (the parameters a
// caller supplies); the flow IR is its OUTPUT. Tooling uses the descriptor to
// render forms and lint configs, and calls `buildFlow` to preview the graph.
//
// Deliberately free of Spark: templates emit node *type ids*, never node
// implementations, so a UI backend can compile a config to IR without Spark
// on its classpath.
// ===========================================================================
plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    api(project(":core"))
    api(libs.typesafeConfig)

    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
}
