package org.openprojectx.spark.boot.core

import java.util.ServiceLoader

/**
 * SPI for libraries that contribute node types to tooling (graph builders,
 * linters, documentation generators).
 *
 * Implementations are discovered with [java.util.ServiceLoader], so a
 * contributing library stays a plain JVM library: no CDI, no Spring, no
 * dependency on any particular tool. Register an implementation by adding its
 * fully qualified name to
 * `META-INF/services/org.openprojectx.spark.boot.core.NodeDescriptorProvider`.
 *
 * Contributing a descriptor is independent from contributing the node
 * *implementation*: descriptors are metadata only and carry no Spark types, so
 * a UI backend can load them without Spark on its classpath.
 */
interface NodeDescriptorProvider {

    /**
     * Stable, human-readable id of the contributing library, for example
     * `spark-boot-connectors` or `lakehouse`. Reported by tooling so operators
     * can tell where a node type came from; not part of the node type id.
     */
    val contributor: String

    /** Node types this library contributes. Must not throw. */
    fun descriptors(): List<NodeDescriptor>
}

/** One provider's contribution, retained so tooling can attribute node types. */
data class NodeContribution(
    val contributor: String,
    val providerClass: String,
    val descriptors: List<NodeDescriptor>,
)

/**
 * The assembled node palette: every [NodeDescriptor] contributed by every
 * discovered [NodeDescriptorProvider], indexed by node type.
 *
 * Built-in nodes are not special-cased — `connectors` registers a provider like
 * any other contributor, so a deployment that omits it simply gets a palette
 * without the built-in nodes.
 */
class NodeCatalog(val contributions: List<NodeContribution>) {

    /** Every contributed descriptor, in contributor order. */
    val descriptors: List<NodeDescriptor> = contributions.flatMap(NodeContribution::descriptors)

    private val byType: Map<String, NodeDescriptor> = descriptors.associateBy(NodeDescriptor::type)

    /**
     * Node types contributed more than once, mapped to the contributors that
     * claimed them. Node type ids are a flat namespace, so a collision means
     * one library shadows another; tooling should surface these rather than
     * silently resolving them (the first contribution wins in [find]).
     */
    val conflicts: Map<String, List<String>> =
        contributions
            .flatMap { contribution -> contribution.descriptors.map { it.type to contribution.contributor } }
            .groupBy({ it.first }, { it.second })
            .filterValues { it.size > 1 }

    fun find(type: String): NodeDescriptor? = byType[type]

    fun contributorOf(type: String): String? =
        contributions.firstOrNull { contribution -> contribution.descriptors.any { it.type == type } }?.contributor

    companion object {
        /** Assembles a catalog from explicitly supplied providers (tests, embedding). */
        fun of(providers: Iterable<NodeDescriptorProvider>): NodeCatalog = NodeCatalog(
            providers.map { provider ->
                NodeContribution(
                    contributor = provider.contributor,
                    providerClass = provider.javaClass.name,
                    descriptors = provider.descriptors(),
                )
            }
        )

        /**
         * Discovers every [NodeDescriptorProvider] visible to [classLoader].
         *
         * Callers should resolve this once and share the result rather than
         * re-running discovery per request: [ServiceLoader] rescans the
         * classpath on each call.
         */
        @JvmOverloads
        fun discover(classLoader: ClassLoader = NodeCatalog::class.java.classLoader): NodeCatalog =
            of(ServiceLoader.load(NodeDescriptorProvider::class.java, classLoader).toList())
    }
}
