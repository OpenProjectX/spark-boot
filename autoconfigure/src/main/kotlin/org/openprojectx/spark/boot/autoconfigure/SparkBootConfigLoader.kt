package org.openprojectx.spark.boot.autoconfigure

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object SparkBootConfigLoader {
    fun load(): SparkBootProperties {
        ConfigFactory.invalidateCaches()
        val applicationConfig = ConfigFactory.defaultApplication()
        val referenceConfig = ConfigFactory.defaultReference()
        val initialConfig = ConfigFactory.defaultOverrides()
            .withFallback(applicationConfig)
            .withFallback(referenceConfig)
            .resolve()
        val activeProfiles = activeProfiles(initialConfig)
        val profiledApplicationConfig = activeProfiles.fold(applicationConfig) { config, profile ->
            ConfigFactory.parseResources("application-$profile.conf").withFallback(config)
        }
        val config = ConfigFactory.defaultOverrides()
            .withFallback(profiledApplicationConfig)
            .withFallback(referenceConfig)
            .resolve()

        if (!config.hasPath("spark.boot")) {
            return SparkBootProperties(activeProfiles = activeProfiles)
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
            activeProfiles = activeProfiles,
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

    private fun activeProfiles(config: Config): Set<String> {
        val configuredProfiles = when {
            config.hasPath("spark.boot.profiles.active") -> config.stringList("spark.boot.profiles.active")
            config.hasPath("spark.profiles.active") -> config.stringList("spark.profiles.active")
            else -> emptyList()
        }

        return (
            configuredProfiles +
                System.getenv("SPARK_BOOT_PROFILES_ACTIVE").splitProfiles() +
                System.getenv("SPARK_PROFILES_ACTIVE").splitProfiles()
            )
            .map(String::trim)
            .filter(String::isNotBlank)
            .toSet()
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

private fun Config.stringList(path: String): List<String> {
    return if (getValue(path).valueType() == com.typesafe.config.ConfigValueType.LIST) {
        getStringList(path)
    } else {
        getString(path).splitProfiles()
    }
}

private fun String?.splitProfiles(): List<String> {
    return this
        ?.split(",", ";")
        ?.map(String::trim)
        ?.filter(String::isNotBlank)
        .orEmpty()
}
