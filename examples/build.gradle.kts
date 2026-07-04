import org.gradle.api.tasks.SourceSetContainer
import java.util.Properties

plugins {
    base
}

val rootGradleProperties = Properties().apply {
    rootProject.layout.projectDirectory.file("../gradle.properties").asFile.inputStream().use(::load)
}
val rootProjectVersion = rootGradleProperties.getProperty("version")
    ?: error("Missing version in root gradle.properties")
extra["sparkBootVersion"] = rootProjectVersion

val runAllTests = tasks.register("runAllTests") {
    group = "verification"
    description = "Runs all test tasks in the examples build."
}

val runAllApps = tasks.register("runAllApps") {
    group = "application"
    description = "Runs all application examples in the examples build."
}

tasks.register("runAll") {
    group = "verification"
    description = "Runs all example tests and application examples."
    dependsOn(runAllTests, runAllApps)
}

runAllApps.configure {
    mustRunAfter(runAllTests)
}

allprojects {
    group = "org.openprojectx.spark.boot.examples"
    version = rootProjectVersion
}

subprojects {
    pluginManager.withPlugin("java") {
        extensions.configure<SourceSetContainer>("sourceSets") {
            named("main") {
                resources.srcDir(rootProject.layout.projectDirectory.dir("src/main/resources"))
            }
        }

        runAllTests.configure {
            dependsOn(tasks.named("test"))
        }
    }

    pluginManager.withPlugin("application") {
        runAllApps.configure {
            dependsOn(tasks.named("run"))
        }
    }
}
