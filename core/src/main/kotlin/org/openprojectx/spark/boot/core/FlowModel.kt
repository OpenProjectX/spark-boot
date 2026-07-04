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

interface UntypedNodeFactory {
    fun create(): FlowNode<*, *>
}

interface NodeFactory<out T : FlowNode<*, *>> : UntypedNodeFactory {
    override fun create(): T
}

class ProgrammaticNodeFactoryRegistry(
    private val factories: Map<String, @JvmSuppressWildcards UntypedNodeFactory>
) {
    @Suppress("UNCHECKED_CAST")
    fun <T : FlowNode<*, *>> create(kind: String): T {
        val factory = factories[kind]
            ?: error("Unknown programmatic node kind: $kind")

        return factory.create() as T
    }
}

interface ConfigNodeFactory {
    fun create(config: Map<String, Any?>): FlowNode<*, *>
}
