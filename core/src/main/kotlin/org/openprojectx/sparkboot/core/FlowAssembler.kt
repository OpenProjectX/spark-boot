package org.openprojectx.sparkboot.core

class FlowAssembler(
    private val registry: NodeFactoryRegistry
) {
    fun assemble(definition: FlowDefinition): ExecutableFlow {
        val nodes = definition.nodes.associate { nodeDefinition ->
            nodeDefinition.id to registry.create(nodeDefinition)
        }

        return ExecutableFlow(
            name = definition.name,
            nodes = nodes,
            edges = definition.edges,
            config = definition.config
        )
    }
}

class NodeFactoryRegistry(
    private val factories: Map<String, @JvmSuppressWildcards ConfigNodeFactory>
) {
    fun create(definition: NodeDefinition): FlowNode<*, *> {
        val factory = factories[definition.type]
            ?: error("Unknown node type: ${definition.type}")

        return factory.create(definition.config)
    }
}
