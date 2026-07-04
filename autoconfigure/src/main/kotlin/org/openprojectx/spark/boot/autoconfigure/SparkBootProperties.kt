package org.openprojectx.spark.boot.autoconfigure

data class SparkBootProperties(
    val activeProfiles: Set<String> = emptySet(),
    val s3: S3Properties? = null,
    val hms: HmsProperties? = null,
    val jdbcConnections: Map<String, JdbcConnectionProperties> = emptyMap(),
    val icebergCatalogs: Map<String, IcebergCatalogProperties> = emptyMap()
)

data class S3Properties(
    val endpoint: String? = null,
    val region: String = "us-east-1",
    val accessKey: String? = null,
    val secretKey: String? = null,
    val pathStyleAccess: Boolean = true,
    val sslEnabled: Boolean = false,
    val credentialsProvider: String = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

data class HmsProperties(
    val uri: String,
    val warehouse: String = "file:/tmp/spark-boot-iceberg-warehouse",
    val catalog: String = "hms"
)

data class JdbcConnectionProperties(
    val url: String,
    val user: String,
    val password: String,
    val driver: String? = null
)

data class IcebergCatalogProperties(
    val name: String,
    val type: String = "hive",
    val uri: String? = null,
    val warehouse: String? = null,
    val properties: Map<String, String> = emptyMap()
)
