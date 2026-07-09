package org.openprojectx.spark.boot.core

data class FlowDefinition(
    val name: String,
    val nodes: List<NodeDefinition>,
    val edges: List<EdgeDefinition>,
    val config: Map<String, Any?> = emptyMap()
)

data class FlowDocument(
    val schemaVersion: String = FLOW_DOCUMENT_SCHEMA_VERSION,
    val flow: FlowDefinition,
    val ui: FlowDocumentUi = FlowDocumentUi()
)

data class FlowDocumentUi(
    val nodes: Map<String, FlowNodeUi> = emptyMap()
)

data class FlowNodeUi(
    val x: Int = 0,
    val y: Int = 0
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

data class NodeDescriptor(
    val type: String,
    val label: String,
    val role: NodeRole,
    val category: String,
    val description: String,
    val config: List<ConfigFieldDescriptor> = emptyList(),
    val minInputs: Int = role.minInputs,
    val maxInputs: Int = role.maxInputs,
    val minOutputs: Int = role.minOutputs,
    val maxOutputs: Int? = role.maxOutputs
)

enum class NodeRole(
    val minInputs: Int,
    val maxInputs: Int,
    val minOutputs: Int,
    val maxOutputs: Int?
) {
    SOURCE(minInputs = 0, maxInputs = 0, minOutputs = 1, maxOutputs = null),
    TRANSFORM(minInputs = 1, maxInputs = 1, minOutputs = 1, maxOutputs = null),
    SINK(minInputs = 1, maxInputs = 1, minOutputs = 0, maxOutputs = 0)
}

data class ConfigFieldDescriptor(
    val key: String,
    val label: String,
    val type: ConfigFieldType,
    val required: Boolean = false,
    val description: String = "",
    val defaultValue: String? = null,
    val options: List<String> = emptyList(),
    val secret: Boolean = false
)

enum class ConfigFieldType {
    STRING,
    STRING_LIST,
    BOOLEAN,
    SAVE_MODE
}

data class FlowValidationDiagnostic(
    val severity: FlowValidationSeverity,
    val code: String,
    val message: String,
    val nodeId: String? = null,
    val field: String? = null
)

enum class FlowValidationSeverity {
    ERROR,
    WARNING
}

class FlowDefinitionValidator(
    descriptors: List<NodeDescriptor>
) {
    private val descriptorsByType = descriptors.associateBy(NodeDescriptor::type)

    fun validate(document: FlowDocument): List<FlowValidationDiagnostic> {
        val diagnostics = mutableListOf<FlowValidationDiagnostic>()
        if (document.schemaVersion != FLOW_DOCUMENT_SCHEMA_VERSION) {
            diagnostics += FlowValidationDiagnostic(
                severity = FlowValidationSeverity.ERROR,
                code = "unknown_schema_version",
                message = "Unsupported flow document schema version: ${document.schemaVersion}"
            )
        }
        diagnostics += validate(document.flow)
        document.ui.nodes.keys
            .filterNot { nodeId -> document.flow.nodes.any { it.id == nodeId } }
            .sorted()
            .forEach { nodeId ->
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.WARNING,
                    code = "unknown_ui_node",
                    message = "UI metadata references unknown node: $nodeId",
                    nodeId = nodeId
                )
            }
        return diagnostics
    }

    fun validate(flow: FlowDefinition): List<FlowValidationDiagnostic> {
        val diagnostics = mutableListOf<FlowValidationDiagnostic>()
        if (flow.name.isBlank()) {
            diagnostics += FlowValidationDiagnostic(
                severity = FlowValidationSeverity.ERROR,
                code = "missing_flow_name",
                message = "Flow name is required"
            )
        }
        if (flow.nodes.isEmpty()) {
            diagnostics += FlowValidationDiagnostic(
                severity = FlowValidationSeverity.ERROR,
                code = "empty_flow",
                message = "Flow must contain at least one node"
            )
        }

        val nodesById = flow.nodes.groupBy(NodeDefinition::id)
        nodesById.filterValues { it.size > 1 }.keys.sorted().forEach { nodeId ->
            diagnostics += FlowValidationDiagnostic(
                severity = FlowValidationSeverity.ERROR,
                code = "duplicate_node_id",
                message = "Duplicate node id: $nodeId",
                nodeId = nodeId
            )
        }

        flow.nodes.forEach { node ->
            if (node.id.isBlank()) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "missing_node_id",
                    message = "Node id is required"
                )
            }

            val descriptor = descriptorsByType[node.type]
            if (descriptor == null) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "unknown_node_type",
                    message = "Unknown node type: ${node.type}",
                    nodeId = node.id
                )
            } else {
                diagnostics += validateNodeConfig(node, descriptor)
            }
        }

        flow.edges.forEach { edge ->
            if (edge.from !in nodesById) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "unknown_edge_source",
                    message = "Edge references unknown source node: ${edge.from}",
                    nodeId = edge.from
                )
            }
            if (edge.to !in nodesById) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "unknown_edge_target",
                    message = "Edge references unknown target node: ${edge.to}",
                    nodeId = edge.to
                )
            }
        }

        if (diagnostics.none { it.code == "duplicate_node_id" }) {
            diagnostics += validatePorts(flow)
            diagnostics += validateAcyclic(flow)
        }

        return diagnostics
    }

    private fun validateNodeConfig(
        node: NodeDefinition,
        descriptor: NodeDescriptor
    ): List<FlowValidationDiagnostic> {
        val diagnostics = mutableListOf<FlowValidationDiagnostic>()
        val fieldsByKey = descriptor.config.associateBy(ConfigFieldDescriptor::key)

        descriptor.config.filter(ConfigFieldDescriptor::required).forEach { field ->
            val value = node.config[field.key]
            if (value == null || value.toString().isBlank()) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "missing_required_config",
                    message = "Node '${node.id}' is missing required config: ${field.key}",
                    nodeId = node.id,
                    field = field.key
                )
            }
        }

        node.config.keys.filterNot { it in fieldsByKey }.sorted().forEach { key ->
            diagnostics += FlowValidationDiagnostic(
                severity = FlowValidationSeverity.WARNING,
                code = "unknown_config_key",
                message = "Node '${node.id}' has an unknown config key: $key",
                nodeId = node.id,
                field = key
            )
        }

        node.config.forEach { (key, value) ->
            val field = fieldsByKey[key] ?: return@forEach
            if (field.options.isNotEmpty() && value != null && value.toString() !in field.options) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "invalid_config_option",
                    message = "Node '${node.id}' config '$key' must be one of: ${field.options.joinToString()}",
                    nodeId = node.id,
                    field = key
                )
            }
            if (field.type == ConfigFieldType.STRING_LIST && value != null && value !is List<*> && value !is String) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "invalid_config_type",
                    message = "Node '${node.id}' config '$key' must be a list or comma-separated string",
                    nodeId = node.id,
                    field = key
                )
            }
        }

        return diagnostics
    }

    private fun validatePorts(flow: FlowDefinition): List<FlowValidationDiagnostic> {
        val incoming = flow.edges.groupBy(EdgeDefinition::to)
        val outgoing = flow.edges.groupBy(EdgeDefinition::from)

        return flow.nodes.flatMap { node ->
            val descriptor = descriptorsByType[node.type] ?: return@flatMap emptyList()
            val diagnostics = mutableListOf<FlowValidationDiagnostic>()
            val inputCount = incoming[node.id].orEmpty().size
            val outputCount = outgoing[node.id].orEmpty().size

            if (inputCount < descriptor.minInputs || inputCount > descriptor.maxInputs) {
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "invalid_input_count",
                    message = "Node '${node.id}' expects ${descriptor.minInputs}..${descriptor.maxInputs} inputs but has $inputCount",
                    nodeId = node.id
                )
            }

            val maxOutputs = descriptor.maxOutputs
            if (outputCount < descriptor.minOutputs || (maxOutputs != null && outputCount > maxOutputs)) {
                val maxOutputText = maxOutputs?.toString() ?: "many"
                diagnostics += FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "invalid_output_count",
                    message = "Node '${node.id}' expects ${descriptor.minOutputs}..$maxOutputText outputs but has $outputCount",
                    nodeId = node.id
                )
            }

            diagnostics
        }
    }

    private fun validateAcyclic(flow: FlowDefinition): List<FlowValidationDiagnostic> {
        val nodes = flow.nodes.map(NodeDefinition::id).toSet()
        val incoming = flow.edges.groupBy(EdgeDefinition::to)
        val outgoing = flow.edges.groupBy(EdgeDefinition::from)
        val inDegree = nodes.associateWith { incoming[it].orEmpty().size }.toMutableMap()
        val ready = ArrayDeque(inDegree.filterValues { it == 0 }.keys.sorted())
        val ordered = mutableListOf<String>()

        while (ready.isNotEmpty()) {
            val node = ready.removeFirst()
            ordered += node
            outgoing[node].orEmpty().map(EdgeDefinition::to).sorted().forEach { target ->
                val next = inDegree.getValue(target) - 1
                inDegree[target] = next
                if (next == 0) {
                    ready += target
                }
            }
        }

        return if (ordered.size == nodes.size) {
            emptyList()
        } else {
            listOf(
                FlowValidationDiagnostic(
                    severity = FlowValidationSeverity.ERROR,
                    code = "cycle",
                    message = "Flow contains a cycle"
                )
            )
        }
    }
}

const val FLOW_DOCUMENT_SCHEMA_VERSION = "spark-boot.flow/v1"

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
