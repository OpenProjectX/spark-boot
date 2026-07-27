package org.openprojectx.spark.boot.core

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FlowDefinitionValidatorActionTest {
    private val validator = FlowDefinitionValidator(
        listOf(
            descriptor("Source", NodeRole.SOURCE),
            descriptor("Transform", NodeRole.TRANSFORM),
            descriptor("Sink", NodeRole.SINK),
            descriptor("SqlAction", NodeRole.ACTION)
        )
    )

    @Test
    fun `action with no inputs or outputs is valid`() {
        val diagnostics = validator.validate(
            FlowDefinition(
                name = "action-only",
                nodes = listOf(NodeDefinition("init", "SqlAction")),
                edges = emptyList()
            )
        )

        assertNoErrors(diagnostics)
    }

    @Test
    fun `flow containing only chained actions is valid`() {
        val diagnostics = validator.validate(
            FlowDefinition(
                name = "actions",
                nodes = listOf(
                    NodeDefinition("first", "SqlAction"),
                    NodeDefinition("second", "SqlAction")
                ),
                edges = listOf(EdgeDefinition("first", "second"))
            )
        )

        assertNoErrors(diagnostics)
    }

    @Test
    fun `sink to action is a valid control edge`() {
        val diagnostics = validator.validate(
            FlowDefinition(
                name = "post-sink-action",
                nodes = listOf(
                    NodeDefinition("orders", "Source"),
                    NodeDefinition("sink", "Sink"),
                    NodeDefinition("expire", "SqlAction")
                ),
                edges = listOf(
                    EdgeDefinition("orders", "sink"),
                    EdgeDefinition("sink", "expire")
                )
            )
        )

        assertNoErrors(diagnostics)
    }

    @Test
    fun `two sinks can order one action`() {
        val diagnostics = validator.validate(
            FlowDefinition(
                name = "join-control",
                nodes = listOf(
                    NodeDefinition("orders", "Source"),
                    NodeDefinition("paid-sink", "Sink"),
                    NodeDefinition("cancelled-sink", "Sink"),
                    NodeDefinition("after-both", "SqlAction")
                ),
                edges = listOf(
                    EdgeDefinition("orders", "paid-sink"),
                    EdgeDefinition("orders", "cancelled-sink"),
                    EdgeDefinition("paid-sink", "after-both"),
                    EdgeDefinition("cancelled-sink", "after-both")
                )
            )
        )

        assertNoErrors(diagnostics)
    }

    @Test
    fun `action cannot feed data node`() {
        val diagnostics = validator.validate(
            FlowDefinition(
                name = "invalid-action-output",
                nodes = listOf(
                    NodeDefinition("init", "SqlAction"),
                    NodeDefinition("transform", "Transform"),
                    NodeDefinition("sink", "Sink")
                ),
                edges = listOf(
                    EdgeDefinition("init", "transform"),
                    EdgeDefinition("transform", "sink")
                )
            )
        )

        assertTrue(diagnostics.any { it.code == "action_edge_to_data_node" }, diagnostics.toString())
    }

    private fun assertNoErrors(diagnostics: List<FlowValidationDiagnostic>) {
        assertFalse(diagnostics.any { it.severity == FlowValidationSeverity.ERROR }, diagnostics.toString())
    }

    private fun descriptor(type: String, role: NodeRole): NodeDescriptor {
        return NodeDescriptor(
            type = type,
            label = type,
            role = role,
            category = role.name,
            description = type
        )
    }
}
