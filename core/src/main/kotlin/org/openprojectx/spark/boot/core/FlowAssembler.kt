package org.openprojectx.spark.boot.core

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
    factories: Map<String, @JvmSuppressWildcards ConfigNodeFactory>,
    profiledFactories: Set<@JvmSuppressWildcards ProfiledConfigNodeFactory> = emptySet(),
    activeProfiles: Set<String> = emptySet()
) {
    private val factories: Map<String, @JvmSuppressWildcards ConfigNodeFactory> =
        mergeProfiledFactories(
            baseFactories = factories,
            profiledFactories = profiledFactories,
            activeProfiles = activeProfiles,
            kind = ProfiledConfigNodeFactory::type,
            profiles = ProfiledConfigNodeFactory::profiles,
            factory = ProfiledConfigNodeFactory::factory,
            duplicateMessage = "config node type"
        )

    fun create(definition: NodeDefinition): FlowNode<*, *> {
        val factory = factories[definition.type]
            ?: error("Unknown node type: ${definition.type}")

        return factory.create(definition.config)
    }
}
