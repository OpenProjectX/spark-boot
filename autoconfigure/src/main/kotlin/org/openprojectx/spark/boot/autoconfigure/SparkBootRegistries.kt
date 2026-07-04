package org.openprojectx.spark.boot.autoconfigure

class JdbcConnectionRegistry(
    private val connections: Map<String, JdbcConnectionProperties>
) {
    fun find(name: String): JdbcConnectionProperties? = connections[name]

    fun get(name: String): JdbcConnectionProperties {
        return find(name) ?: error("Unknown Spark Boot JDBC connection: $name")
    }
}

class IcebergCatalogRegistry(
    private val catalogs: Map<String, IcebergCatalogProperties>
) {
    fun find(name: String): IcebergCatalogProperties? = catalogs[name]

    fun get(name: String): IcebergCatalogProperties {
        return find(name) ?: error("Unknown Spark Boot Iceberg catalog: $name")
    }
}
