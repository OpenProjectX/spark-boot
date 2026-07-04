package org.openprojectx.spark.boot.core

data class FlowDefinition(
    val name: String,
    val nodes: List<NodeDefinition>,
    val edges: List<EdgeDefinition>,
    val config: Map<String, Any?> = emptyMap()
)

data class NodeDefinition(
    val id: String,
    val type: String,
    val config: Map<String, Any?> = emptyMap()
)

data class EdgeDefinition(
    val from: String,
    val to: String
)

data class ExecutableFlow(
    val name: String,
    val nodes: Map<String, FlowNode<*, *>>,
    val edges: List<EdgeDefinition>,
    val config: Map<String, Any?> = emptyMap()
)

interface FlowNode<I, O> {
    val name: String
}

interface NodeFactory<T : FlowNode<*, *>> {
    fun create(): T
}

interface ConfigNodeFactory {
    fun create(config: Map<String, Any?>): FlowNode<*, *>
}
