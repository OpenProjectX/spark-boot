package org.openprojectx.spark.boot.template

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openprojectx.spark.boot.core.ConfigFieldDescriptor
import org.openprojectx.spark.boot.core.ConfigFieldType
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.core.FlowDefinition
import org.openprojectx.spark.boot.core.NodeDefinition

/**
 * Pins the two properties tooling relies on: a template is a pure
 * `Config -> FlowDefinition` compiler whose graph may depend on the config,
 * and its descriptor is the single source of truth for its identity.
 */
class TemplateCatalogTest {

    /** Minimal template whose topology is config-dependent, like real ones. */
    private object ExampleTemplate : JobTemplate {
        override val descriptor = JobDescriptor(
            name = "example",
            label = "Example",
            schemaVersion = 2,
            fields = listOf(
                ConfigFieldDescriptor("source.path", "Path", ConfigFieldType.STRING, required = true, section = "source"),
                ConfigFieldDescriptor("source.where", "Filter", ConfigFieldType.STRING, section = "source"),
            ),
        )

        override fun buildFlow(config: Config): FlowDefinition {
            if (!config.hasPath("source.path")) {
                throw JobConfigException("Missing required config 'source.path'", field = "source.path")
            }
            val nodes = mutableListOf(
                NodeDefinition("source", "ParquetSource", mapOf("path" to config.getString("source.path"))),
            )
            if (config.hasPath("source.where")) {
                nodes += NodeDefinition("filter", "SqlFilterTransform", mapOf("condition" to config.getString("source.where")))
            }
            val ids = nodes.map { it.id }
            return FlowDefinition("example", nodes, ids.zip(ids.drop(1)).map { EdgeDefinition(it.first, it.second) })
        }
    }

    private val provider = object : JobTemplateProvider {
        override val contributor = "test"
        override fun templates() = listOf<JobTemplate>(ExampleTemplate)
    }

    @Test
    fun `identity is derived from the descriptor so the two cannot drift`() {
        assertEquals("example", ExampleTemplate.name)
        assertEquals(2, ExampleTemplate.schemaVersion)
    }

    @Test
    fun `topology depends on the config, so the IR is compiled and not substituted`() {
        val bare = ExampleTemplate.buildFlow(ConfigFactory.parseString("""source { path = "/data" }"""))
        assertEquals(listOf("source"), bare.nodes.map { it.id })

        val filtered = ExampleTemplate.buildFlow(
            ConfigFactory.parseString("""source { path = "/data", where = "x > 1" }"""),
        )
        assertEquals(listOf("source", "filter"), filtered.nodes.map { it.id })
        assertEquals(listOf(EdgeDefinition("source", "filter")), filtered.edges)
    }

    @Test
    fun `default validate reports the failing field so a form can anchor it`() {
        val diagnostics = ExampleTemplate.validate(ConfigFactory.empty())

        assertEquals(1, diagnostics.size)
        assertEquals("source.path", diagnostics.single().field)
        assertTrue(diagnostics.single().message.contains("source.path"))
        assertTrue(ExampleTemplate.validate(ConfigFactory.parseString("""source { path = "/data" }""")).isEmpty())
    }

    @Test
    fun `catalog resolves templates and attributes them to a contributor`() {
        val catalog = TemplateCatalog.of(listOf(provider))

        assertEquals(listOf("example"), catalog.names())
        assertEquals("test", catalog.contributorOf("example"))
        assertEquals(ExampleTemplate, catalog.require("example"))
        assertTrue(catalog.conflicts.isEmpty())
    }

    @Test
    fun `an unknown template names the available ones`() {
        val catalog = TemplateCatalog.of(listOf(provider))

        val error = assertThrows<JobConfigException> { catalog.require("nope") }
        assertTrue(error.message!!.contains("example"))
    }
}
