package org.openprojectx.spark.boot.template

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.FlowDefinition

/**
 * A parameterised flow: turns a submitted configuration into a
 * [FlowDefinition]. Templates are the layer at which non-authors consume a
 * pipeline — they fill in parameters instead of wiring nodes.
 *
 * A template is a *compiler*, not a stencil: the graph it produces may depend
 * on the config (optional steps appearing, identifiers being derived), so the
 * IR cannot be expressed as a graph with placeholders. This also means
 * [buildFlow] is not invertible — tools must treat a generated flow as
 * read-only output and keep the config as the artifact of record.
 *
 * Implementations must be deterministic and side-effect free: tooling calls
 * [buildFlow] on every keystroke to preview the graph.
 */
interface JobTemplate {

    /** Metadata describing this template and the parameters it accepts. */
    val descriptor: JobDescriptor

    /** Stable template id, referenced by `job.template`. */
    val name: String get() = descriptor.name

    /** Config schema version this template implements. */
    val schemaVersion: Int get() = descriptor.schemaVersion

    /**
     * Compiles [config] into a flow, rejecting invalid configuration
     * fail-fast with [JobConfigException].
     */
    fun buildFlow(config: Config): FlowDefinition

    /**
     * Collects configuration problems without building a flow.
     *
     * The default implementation compiles the config and reports the first
     * failure, which is enough for a CLI but poor for a form: override to
     * report every field at once. Returning an empty list means [buildFlow]
     * will succeed for this config.
     */
    fun validate(config: Config): List<JobConfigDiagnostic> =
        try {
            buildFlow(config)
            emptyList()
        } catch (e: JobConfigException) {
            listOf(JobConfigDiagnostic.error(e.message ?: "Invalid configuration", field = e.field))
        }
}

/**
 * Raised when a submitted configuration does not satisfy a template's schema.
 *
 * The message must be actionable on its own: configs are typically authored in
 * a different repository from the template, so this text is often the only
 * feedback the author gets. [field] anchors the problem to a config path so
 * form UIs can highlight the offending input in place.
 */
open class JobConfigException(
    message: String,
    val field: String? = null,
) : RuntimeException(message)
