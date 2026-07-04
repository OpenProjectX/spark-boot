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
    }
}
