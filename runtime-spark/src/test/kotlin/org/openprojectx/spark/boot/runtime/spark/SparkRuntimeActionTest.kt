package org.openprojectx.spark.boot.runtime.spark

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.core.ExecutableFlow
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.core.FlowDefinition
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeDefinition
import org.openprojectx.spark.boot.core.NodeFactoryRegistry

class SparkRuntimeActionTest {
    @Test
    fun `source sink action executes action last and receives unit`() {
        val events = mutableListOf<String>()
        val flow = ExecutableFlow(
            name = "ordered",
            nodes = mapOf(
                "source" to RecordingSourceNode(events),
                "sink" to RecordingSinkNode("sink", events),
                "action" to RecordingActionNode("action", events)
            ),
            edges = listOf(
                EdgeDefinition("source", "sink"),
                EdgeDefinition("sink", "action")
            )
        )

        runtime.run(flow)

        assertEquals(listOf("source", "sink:data", "action:unit"), events)
    }

    @Test
    fun `action with two incoming control edges runs after both upstream nodes`() {
        val events = mutableListOf<String>()
        val flow = ExecutableFlow(
            name = "multi-input-action",
            nodes = mapOf(
                "source" to RecordingSourceNode(events),
                "sink-a" to RecordingSinkNode("sink-a", events),
                "sink-b" to RecordingSinkNode("sink-b", events),
                "action" to RecordingActionNode("action", events)
            ),
            edges = listOf(
                EdgeDefinition("source", "sink-a"),
                EdgeDefinition("source", "sink-b"),
                EdgeDefinition("sink-a", "action"),
                EdgeDefinition("sink-b", "action")
            )
        )

        runtime.run(flow)

        assertEquals(listOf("source", "sink-a:data", "sink-b:data", "action:unit"), events)
    }

    @Test
    fun `action-only executable flow runs`() {
        val events = mutableListOf<String>()
        val flow = ExecutableFlow(
            name = "action-only",
            nodes = mapOf("action" to RecordingActionNode("action", events)),
            edges = emptyList()
        )

        runtime.run(flow)

        assertEquals(listOf("action:unit"), events)
    }

    @Test
    fun `custom config action factory assembles and runs`() {
        val events = mutableListOf<String>()
        val registry = NodeFactoryRegistry(
            mapOf(
                "MyThingAction" to object : ConfigNodeFactory {
                    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
                        return RecordingActionNode(config.getValue("label").toString(), events)
                    }
                }
            )
        )
        val flow = FlowAssembler(registry).assemble(
            FlowDefinition(
                name = "custom-action",
                nodes = listOf(
                    NodeDefinition(
                        id = "custom",
                        type = "MyThingAction",
                        config = mapOf("label" to "custom")
                    )
                ),
                edges = emptyList()
            )
        )

        runtime.run(flow)

        assertEquals(listOf("custom:unit"), events)
    }

    private class RecordingSourceNode(
        private val events: MutableList<String>
    ) : SparkSourceNode<String> {
        override val name: String = "recording-source"

        override fun execute(input: Unit, context: SparkExecutionContext): String {
            events += "source"
            return "data"
        }
    }

    private class RecordingSinkNode(
        private val id: String,
        private val events: MutableList<String>
    ) : SparkSinkNode<String> {
        override val name: String = "recording-sink"

        override fun execute(input: String, context: SparkExecutionContext) {
            events += "$id:$input"
        }
    }

    private class RecordingActionNode(
        private val id: String,
        private val events: MutableList<String>
    ) : SparkActionNode {
        override val name: String = "recording-action"

        override fun execute(input: Unit, context: SparkExecutionContext) {
            events += "$id:unit"
        }
    }

    companion object {
        private val spark = SparkSession.builder()
            .appName("spark-runtime-action-test")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        private val runtime = SparkRuntime(SparkExecutionContext(spark))

        @JvmStatic
        @AfterAll
        fun stopSpark() {
            spark.stop()
        }
    }
}
