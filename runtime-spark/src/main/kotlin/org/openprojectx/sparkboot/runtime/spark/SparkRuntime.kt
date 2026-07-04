package org.openprojectx.sparkboot.runtime.spark

import org.apache.spark.sql.SparkSession
import org.openprojectx.sparkboot.core.EdgeDefinition
import org.openprojectx.sparkboot.core.ExecutableFlow
import org.openprojectx.sparkboot.core.FlowNode

data class SparkExecutionContext(
    val spark: SparkSession,
    val config: Map<String, Any?> = emptyMap()
)

interface SparkNode<I, O> : FlowNode<I, O> {
    fun execute(input: I, context: SparkExecutionContext): O
}

interface SparkSourceNode<O> : SparkNode<Unit, O>

interface SparkTransformNode<I, O> : SparkNode<I, O>

interface SparkSinkNode<I> : SparkNode<I, Unit>

class SparkRuntime(
    private val context: SparkExecutionContext
) {
    fun run(flow: ExecutableFlow) {
        val graph = DagGraph.from(flow)
        graph.validate()

        val results = mutableMapOf<String, Any?>()

        for (nodeId in graph.topologicalOrder()) {
            val node = flow.nodes[nodeId]
                ?: error("Node not found: $nodeId")
            val input = graph.resolveInput(nodeId, results)
            results[nodeId] = executeNode(node, input)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun executeNode(node: FlowNode<*, *>, input: Any?): Any? {
        val sparkNode = node as? SparkNode<Any?, Any?>
            ?: error("Node is not a SparkNode: ${node.name}")

        return sparkNode.execute(input, context)
    }
}

class DagGraph private constructor(
    private val nodes: Set<String>,
    private val edges: List<EdgeDefinition>
) {
    private val incoming = edges.groupBy { it.to }
    private val outgoing = edges.groupBy { it.from }

    fun validate() {
        require(nodes.isNotEmpty()) { "Flow must contain at least one node" }

        edges.forEach { edge ->
            require(edge.from in nodes) { "Edge references unknown source node: ${edge.from}" }
            require(edge.to in nodes) { "Edge references unknown target node: ${edge.to}" }
        }

        val sourceCount = nodes.count { incoming[it].isNullOrEmpty() }
        val sinkCount = nodes.count { outgoing[it].isNullOrEmpty() }
        require(sourceCount > 0) { "Flow must contain at least one source node" }
        require(sinkCount > 0) { "Flow must contain at least one sink node" }

        incoming.forEach { (nodeId, incomingEdges) ->
            require(incomingEdges.size <= 1) {
                "Node $nodeId has ${incomingEdges.size} inputs; v1 supports only one input per node"
            }
        }

        topologicalOrder()
    }

    fun topologicalOrder(): List<String> {
        val inDegree = nodes.associateWith { incoming[it]?.size ?: 0 }.toMutableMap()
        val ready = ArrayDeque(inDegree.filterValues { it == 0 }.keys.sorted())
        val ordered = mutableListOf<String>()

        while (ready.isNotEmpty()) {
            val node = ready.removeFirst()
            ordered += node

            outgoing[node].orEmpty().map { it.to }.sorted().forEach { target ->
                val nextDegree = inDegree.getValue(target) - 1
                inDegree[target] = nextDegree
                if (nextDegree == 0) {
                    ready += target
                }
            }
        }

        require(ordered.size == nodes.size) { "Flow contains a cycle" }
        return ordered
    }

    fun resolveInput(nodeId: String, results: Map<String, Any?>): Any? {
        val incomingEdges = incoming[nodeId].orEmpty()
        return when (incomingEdges.size) {
            0 -> Unit
            1 -> results[incomingEdges.single().from]
            else -> error("Node $nodeId has ${incomingEdges.size} inputs; v1 supports only one input per node")
        }
    }

    companion object {
        fun from(flow: ExecutableFlow): DagGraph {
            require(flow.nodes.size == flow.nodes.keys.toSet().size) { "Duplicate node ids are not allowed" }
            return DagGraph(flow.nodes.keys, flow.edges)
        }
    }
}
