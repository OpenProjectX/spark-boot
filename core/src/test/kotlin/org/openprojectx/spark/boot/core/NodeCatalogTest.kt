package org.openprojectx.spark.boot.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class NodeCatalogTest {

    private fun descriptor(type: String) = NodeDescriptor(
        type = type,
        label = type,
        role = NodeRole.SOURCE,
        category = "Source",
        description = "",
    )

    private fun provider(name: String, vararg types: String) = object : NodeDescriptorProvider {
        override val contributor = name
        override fun descriptors() = types.map(::descriptor)
    }

    @Test
    fun `aggregates descriptors from every provider and attributes them`() {
        val catalog = NodeCatalog.of(
            listOf(provider("builtin", "ParquetSource"), provider("lakehouse", "IcebergMergeSink")),
        )

        assertEquals(listOf("ParquetSource", "IcebergMergeSink"), catalog.descriptors.map { it.type })
        assertEquals("lakehouse", catalog.contributorOf("IcebergMergeSink"))
        assertEquals("ParquetSource", catalog.find("ParquetSource")?.type)
        assertNull(catalog.find("NoSuchNode"))
    }

    @Test
    fun `reports type collisions instead of silently shadowing`() {
        val catalog = NodeCatalog.of(
            listOf(provider("builtin", "ParquetSource"), provider("lakehouse", "ParquetSource")),
        )

        // Node type ids are a flat namespace: two contributors claiming one id
        // means one shadows the other, which must be visible to operators.
        assertEquals(mapOf("ParquetSource" to listOf("builtin", "lakehouse")), catalog.conflicts)
        assertEquals("builtin", catalog.contributorOf("ParquetSource"))
    }

    @Test
    fun `an empty deployment yields an empty palette rather than failing`() {
        val catalog = NodeCatalog.of(emptyList())

        assertTrue(catalog.descriptors.isEmpty())
        assertTrue(catalog.conflicts.isEmpty())
    }
}
