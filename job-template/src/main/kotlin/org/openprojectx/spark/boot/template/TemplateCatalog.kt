package org.openprojectx.spark.boot.template

import java.util.ServiceLoader

/**
 * SPI for libraries that contribute job templates.
 *
 * Discovered with [java.util.ServiceLoader] like
 * [org.openprojectx.spark.boot.core.NodeDescriptorProvider], so a contributing
 * project stays a plain JVM library. Register an implementation in
 * `META-INF/services/org.openprojectx.spark.boot.template.JobTemplateProvider`.
 */
interface JobTemplateProvider {

    /** Stable id of the contributing library, e.g. `lakehouse`. */
    val contributor: String

    /** Templates this library contributes. Must not throw. */
    fun templates(): List<JobTemplate>
}

/** One provider's contribution, retained so tooling can attribute templates. */
data class TemplateContribution(
    val contributor: String,
    val providerClass: String,
    val templates: List<JobTemplate>,
)

/**
 * The assembled template gallery across every discovered
 * [JobTemplateProvider].
 */
class TemplateCatalog(val contributions: List<TemplateContribution>) {

    val templates: List<JobTemplate> = contributions.flatMap(TemplateContribution::templates)

    private val byName: Map<String, JobTemplate> = templates.associateBy(JobTemplate::name)

    /** Template ids claimed by more than one contributor; the first wins in [find]. */
    val conflicts: Map<String, List<String>> =
        contributions
            .flatMap { contribution -> contribution.templates.map { it.name to contribution.contributor } }
            .groupBy({ it.first }, { it.second })
            .filterValues { it.size > 1 }

    fun names(): List<String> = templates.map(JobTemplate::name).sorted()

    fun find(name: String): JobTemplate? = byName[name]

    fun require(name: String): JobTemplate = byName[name]
        ?: throw JobConfigException("Unknown job template '$name'. Available: ${names().joinToString(", ")}")

    fun contributorOf(name: String): String? =
        contributions.firstOrNull { contribution -> contribution.templates.any { it.name == name } }?.contributor

    companion object {
        fun of(providers: Iterable<JobTemplateProvider>): TemplateCatalog = TemplateCatalog(
            providers.map { provider ->
                TemplateContribution(
                    contributor = provider.contributor,
                    providerClass = provider.javaClass.name,
                    templates = provider.templates(),
                )
            }
        )

        /** Resolve once and share: [ServiceLoader] rescans the classpath per call. */
        @JvmOverloads
        fun discover(classLoader: ClassLoader = TemplateCatalog::class.java.classLoader): TemplateCatalog =
            of(ServiceLoader.load(JobTemplateProvider::class.java, classLoader).toList())
    }
}
