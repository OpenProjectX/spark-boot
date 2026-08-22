package org.openprojectx.spark.boot.connectors

import org.openprojectx.spark.boot.core.NodeDescriptor
import org.openprojectx.spark.boot.core.NodeDescriptorProvider

/**
 * Contributes the built-in node palette through the same SPI third-party
 * libraries use. Built-ins are deliberately not special-cased: a deployment
 * that leaves `connectors` off the classpath simply gets a palette without
 * them, which keeps the discovery path honest.
 */
class BuiltinNodeDescriptorProvider : NodeDescriptorProvider {
    override val contributor: String = CONTRIBUTOR
    override fun descriptors(): List<NodeDescriptor> = BuiltinNodeDescriptors.all

    companion object {
        const val CONTRIBUTOR = "spark-boot-connectors"
    }
}
