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
    factories: Map<String, @JvmSuppressWildcards UntypedNodeFactory>,
    profiledFactories: Set<@JvmSuppressWildcards ProfiledProgrammaticNodeFactory> = emptySet(),
    activeProfiles: Set<String> = emptySet()
) {
    private val factories: Map<String, @JvmSuppressWildcards UntypedNodeFactory> =
        mergeProfiledFactories(
            baseFactories = factories,
            profiledFactories = profiledFactories,
            activeProfiles = activeProfiles,
            kind = ProfiledProgrammaticNodeFactory::kind,
            profiles = ProfiledProgrammaticNodeFactory::profiles,
            factory = ProfiledProgrammaticNodeFactory::factory,
            duplicateMessage = "programmatic node kind"
        )

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

data class ProfiledProgrammaticNodeFactory(
    val kind: String,
    val profiles: Set<String>,
    val factory: UntypedNodeFactory
)

data class ProfiledConfigNodeFactory(
    val type: String,
    val profiles: Set<String>,
    val factory: ConfigNodeFactory
)

internal fun <P, F> mergeProfiledFactories(
    baseFactories: Map<String, F>,
    profiledFactories: Set<P>,
    activeProfiles: Set<String>,
    kind: (P) -> String,
    profiles: (P) -> Set<String>,
    factory: (P) -> F,
    duplicateMessage: String
): Map<String, F> {
    val activeProfileSet = activeProfiles.filter(String::isNotBlank).toSet()
    val activeProfiledFactories = profiledFactories.filter { contribution ->
        profiles(contribution).any { profile -> profile in activeProfileSet }
    }

    val duplicateKinds = activeProfiledFactories
        .groupingBy(kind)
        .eachCount()
        .filterValues { count -> count > 1 }
        .keys

    require(duplicateKinds.isEmpty()) {
        "Multiple active profiled factories matched the same $duplicateMessage: ${duplicateKinds.sorted()}"
    }

    return baseFactories + activeProfiledFactories.associate { contribution ->
        kind(contribution) to factory(contribution)
    }
}
