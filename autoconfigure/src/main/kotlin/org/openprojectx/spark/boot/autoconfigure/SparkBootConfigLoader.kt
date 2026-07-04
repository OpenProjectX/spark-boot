package org.openprojectx.spark.boot.autoconfigure

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object SparkBootConfigLoader {
    fun load(): SparkBootProperties {
        ConfigFactory.invalidateCaches()
        val config = ConfigFactory.load()
        if (!config.hasPath("spark.boot")) {
            return SparkBootProperties()
        }

        val sparkBoot = config.getConfig("spark.boot")
        val hms = sparkBoot.optionalConfig("hms")?.let { hmsConfig ->
            HmsProperties(
                uri = hmsConfig.requiredString("uri"),
                warehouse = hmsConfig.optionalString("warehouse") ?: "file:/tmp/spark-boot-iceberg-warehouse",
                catalog = hmsConfig.optionalString("catalog") ?: "hms"
            )
        }

        val explicitCatalogs = sparkBoot.optionalConfig("iceberg.catalogs")
            ?.children()
            ?.mapValues { (name, catalog) ->
                IcebergCatalogProperties(
                    name = name,
                    type = catalog.optionalString("type") ?: "hive",
                    uri = catalog.optionalString("uri"),
                    warehouse = catalog.optionalString("warehouse"),
                    properties = catalog.optionalConfig("properties")?.stringMap().orEmpty()
                )
            }
            .orEmpty()

        val hmsCatalog = hms?.let {
            it.catalog to IcebergCatalogProperties(
                name = it.catalog,
                type = "hive",
                uri = it.uri,
                warehouse = it.warehouse
            )
        }

        return SparkBootProperties(
            s3 = sparkBoot.optionalConfig("s3")?.let { s3Config ->
                S3Properties(
                    endpoint = s3Config.optionalString("endpoint"),
                    region = s3Config.optionalString("region") ?: "us-east-1",
                    accessKey = s3Config.optionalString("access-key") ?: s3Config.optionalString("accessKey"),
                    secretKey = s3Config.optionalString("secret-key") ?: s3Config.optionalString("secretKey"),
                    pathStyleAccess = s3Config.optionalBoolean("path-style-access")
                        ?: s3Config.optionalBoolean("pathStyleAccess")
                        ?: true,
                    sslEnabled = s3Config.optionalBoolean("ssl-enabled")
                        ?: s3Config.optionalBoolean("sslEnabled")
                        ?: false,
                    credentialsProvider = s3Config.optionalString("credentials-provider")
                        ?: s3Config.optionalString("credentialsProvider")
                        ?: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                )
            },
            hms = hms,
            jdbcConnections = sparkBoot.optionalConfig("jdbc.connections")
                ?.children()
                ?.mapValues { (_, jdbc) ->
                    JdbcConnectionProperties(
                        url = jdbc.requiredString("url"),
                        user = jdbc.requiredString("user"),
                        password = jdbc.requiredString("password"),
                        driver = jdbc.optionalString("driver")
                    )
                }
                .orEmpty(),
            icebergCatalogs = explicitCatalogs + listOfNotNull(hmsCatalog)
        )
    }
}

private fun Config.children(): Map<String, Config> {
    return root().keys.associateWith { key -> getConfig(key) }
}

private fun Config.optionalConfig(path: String): Config? {
    return if (hasPath(path)) getConfig(path) else null
}

private fun Config.stringMap(): Map<String, String> {
    return root().keys.associateWith { key -> getString(key) }
}

private fun Config.requiredString(path: String): String {
    return optionalString(path) ?: error("Missing required config path: spark.boot.$path")
}

private fun Config.optionalString(path: String): String? {
    return if (hasPath(path)) getString(path).takeIf(String::isNotBlank) else null
}

private fun Config.optionalBoolean(path: String): Boolean? {
    return if (hasPath(path)) getBoolean(path) else null
}
