package org.openprojectx.spark.boot.template

import org.openprojectx.spark.boot.core.ConfigFieldDescriptor
import org.openprojectx.spark.boot.core.FlowValidationSeverity

/**
 * Machine-readable description of a [JobTemplate]: its identity and the
 * parameters it accepts.
 *
 * This is the template-level counterpart of
 * [org.openprojectx.spark.boot.core.NodeDescriptor]. The two describe
 * different things and must not be conflated: a node descriptor describes one
 * box in a graph, while a job descriptor describes the *inputs* to a function
 * that emits a whole graph.
 *
 * One descriptor drives four consumers that would otherwise drift apart:
 * form rendering in a UI, config linting in CI, runtime validation, and
 * generated documentation.
 */
data class JobDescriptor(
    /** Stable template id, e.g. `cdc-silver-merge`. */
    val name: String,
    /** Human-readable name for galleries and forms. */
    val label: String,
    /** Config schema version this template implements. */
    val schemaVersion: Int,
    val description: String = "",
    /**
     * Free-form grouping for galleries, e.g. a medallion layer (`bronze`) or a
     * domain. Left generic so contributing projects can impose their own
     * taxonomy.
     */
    val category: String = "",
    val stability: JobStability = JobStability.STABLE,
    /**
     * Accepted parameters, keyed by dotted config path (`cdc.primary-key`).
     * [ConfigFieldDescriptor.section] should name the top-level block so forms
     * can group fields the way the config file reads.
     */
    val fields: List<ConfigFieldDescriptor> = emptyList(),
    /** Optional labels/descriptions for the sections referenced by [fields]. */
    val sections: List<JobConfigSection> = emptyList(),
    /** Optional complete example config, rendered as a starting point. */
    val example: String = "",
)

/** Presentation metadata for one group of related config fields. */
data class JobConfigSection(
    val key: String,
    val label: String,
    val description: String = "",
)

/**
 * Lifecycle signal for config authors. Tooling should warn on [DEPRECATED]
 * templates and mark [EXPERIMENTAL] ones as subject to breaking change.
 */
enum class JobStability { EXPERIMENTAL, STABLE, DEPRECATED }

/**
 * A configuration problem anchored to a config path.
 *
 * Mirrors [org.openprojectx.spark.boot.core.FlowValidationDiagnostic], which
 * reports problems in the compiled graph; this one reports problems in the
 * parameters that produced it, so a form can highlight the input the author
 * actually typed.
 */
data class JobConfigDiagnostic(
    val severity: FlowValidationSeverity,
    val code: String,
    val message: String,
    /** Dotted config path, e.g. `cdc.primary-key`. Null when flow-wide. */
    val field: String? = null,
) {
    companion object {
        fun error(message: String, field: String? = null, code: String = "job_config_invalid") =
            JobConfigDiagnostic(FlowValidationSeverity.ERROR, code, message, field)

        fun warning(message: String, field: String? = null, code: String = "job_config_warning") =
            JobConfigDiagnostic(FlowValidationSeverity.WARNING, code, message, field)
    }
}
