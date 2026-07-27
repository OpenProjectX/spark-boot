package org.openprojectx.spark.boot.connectors

import javax.inject.Inject
import org.apache.spark.sql.SaveMode
import org.openprojectx.spark.boot.autoconfigure.IcebergCatalogRegistry
import org.openprojectx.spark.boot.autoconfigure.JdbcConnectionRegistry
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory

class ParquetSourceNodeFactory @Inject constructor() : NodeFactory<ParquetSourceNode> {
    override fun create(): ParquetSourceNode = ParquetSourceNode()
}

class ParquetSinkNodeFactory @Inject constructor() : NodeFactory<ParquetSinkNode> {
    override fun create(): ParquetSinkNode = ParquetSinkNode()
}

class KafkaSourceNodeFactory @Inject constructor() : NodeFactory<KafkaSourceNode> {
    override fun create(): KafkaSourceNode = KafkaSourceNode()
}

class JdbcSourceNodeFactory @Inject constructor(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry
) : NodeFactory<JdbcSourceNode> {
    override fun create(): JdbcSourceNode = JdbcSourceNode(jdbcConnectionRegistry)
}

class HudiSourceNodeFactory @Inject constructor() : NodeFactory<HudiSourceNode> {
    override fun create(): HudiSourceNode = HudiSourceNode()
}

class HudiSinkNodeFactory @Inject constructor() : NodeFactory<HudiSinkNode> {
    override fun create(): HudiSinkNode = HudiSinkNode()
}

class IcebergSinkNodeFactory @Inject constructor(
    private val icebergCatalogRegistry: IcebergCatalogRegistry
) : NodeFactory<IcebergSinkNode> {
    override fun create(): IcebergSinkNode = IcebergSinkNode(icebergCatalogRegistry)
}

class SqlFilterNodeFactory @Inject constructor() : NodeFactory<SqlFilterNode> {
    override fun create(): SqlFilterNode = SqlFilterNode()
}

class SelectNodeFactory @Inject constructor() : NodeFactory<SelectNode> {
    override fun create(): SelectNode = SelectNode()
}

class SqlTransformNodeFactory @Inject constructor() : NodeFactory<SqlTransformNode> {
    override fun create(): SqlTransformNode = SqlTransformNode()
}

class JdbcSinkNodeFactory @Inject constructor(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry
) : NodeFactory<JdbcSinkNode> {
    override fun create(): JdbcSinkNode = JdbcSinkNode(jdbcConnectionRegistry)
}

class SqlActionNodeFactory @Inject constructor() : NodeFactory<SqlActionNode> {
    override fun create(): SqlActionNode = SqlActionNode()
}

class ParquetSourceConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return ParquetSourceNode().apply {
            path = requiredString(config, "path")
        }
    }
}

class ParquetSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return ParquetSinkNode().apply {
            path = requiredString(config, "path")
            mode = saveMode(config["save_mode"] ?: config["mode"])
        }
    }
}

class KafkaSourceConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return KafkaSourceNode().apply {
            bootstrapServers = requiredString(config, "bootstrap_servers", "bootstrapServers")
            topic = optionalString(config, "topic")
            subscribe = optionalString(config, "subscribe")
            subscribePattern = optionalString(config, "subscribe_pattern", "subscribePattern")
            assign = optionalString(config, "assign")
            startingOffsets = optionalString(config, "starting_offsets", "startingOffsets") ?: "earliest"
            endingOffsets = optionalString(config, "ending_offsets", "endingOffsets") ?: "latest"
            includeHeaders = optionalBoolean(config, "include_headers", "includeHeaders") ?: false
            failOnDataLoss = optionalBoolean(config, "fail_on_data_loss", "failOnDataLoss") ?: true
            options = optionalStringMap(config, "options")
        }
    }
}

class JdbcSourceConfigFactory @Inject constructor(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry
) : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return JdbcSourceNode(jdbcConnectionRegistry).apply {
            connection = optionalString(config, "connection")
            url = optionalString(config, "url")
            table = requiredString(config, "table")
            user = optionalString(config, "user")
            password = optionalString(config, "password")
            driver = optionalString(config, "driver")
        }
    }
}

class HudiSourceConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return HudiSourceNode().apply {
            path = requiredString(config, "path")
            queryType = optionalString(config, "query_type", "queryType")
            options = optionalStringMap(config, "options")
        }
    }
}

class HudiSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return HudiSinkNode().apply {
            path = requiredString(config, "path")
            table = requiredString(config, "table")
            database = optionalString(config, "database") ?: "default"
            recordKeyField = requiredString(config, "record_key_field", "recordKeyField")
            precombineField = requiredString(config, "precombine_field", "precombineField")
            partitionPathField = optionalString(config, "partition_path_field", "partitionPathField")
            tableType = optionalString(config, "table_type", "tableType") ?: "COPY_ON_WRITE"
            operation = optionalString(config, "operation") ?: "upsert"
            mode = saveMode(config["save_mode"] ?: config["mode"])
            hiveSync = optionalBoolean(config, "hive_sync", "hiveSync") ?: false
            hiveSyncMode = optionalString(config, "hive_sync_mode", "hiveSyncMode") ?: "hms"
            hiveMetastoreUris = optionalString(config, "hive_metastore_uris", "hiveMetastoreUris")
            hiveSyncDatabase = optionalString(config, "hive_sync_database", "hiveSyncDatabase")
            hiveSyncTable = optionalString(config, "hive_sync_table", "hiveSyncTable")
            options = optionalStringMap(config, "options")
        }
    }
}

class IcebergSinkConfigFactory @Inject constructor(
    private val icebergCatalogRegistry: IcebergCatalogRegistry
) : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return IcebergSinkNode(icebergCatalogRegistry).apply {
            catalog = optionalString(config, "catalog")
            table = requiredString(config, "table")
            mode = saveMode(config["save_mode"] ?: config["mode"])
        }
    }
}

class SqlFilterConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return SqlFilterNode().apply {
            condition = requiredString(config, "condition")
        }
    }
}

class SelectConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return SelectNode().apply {
            columns = requiredStringList(config, "columns")
        }
    }
}

class SqlTransformConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return SqlTransformNode().apply {
            pluginInput = requiredString(config, "plugin_input")
            pluginOutput = requiredString(config, "plugin_output")
            query = requiredString(config, "query")
        }
    }
}

class JdbcSinkConfigFactory @Inject constructor(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry
) : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return JdbcSinkNode(jdbcConnectionRegistry).apply {
            connection = optionalString(config, "connection")
            url = optionalString(config, "url")
            table = requiredString(config, "table")
            user = optionalString(config, "user")
            password = optionalString(config, "password")
            mode = saveMode(config["save_mode"] ?: config["mode"], SaveMode.Append)
        }
    }
}

class SqlActionConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return SqlActionNode().apply {
            sql = requiredString(config, "sql")
            label = optionalString(config, "label")
        }
    }
}

private fun requiredString(config: Map<String, Any?>, vararg keys: String): String {
    return firstValue(config, *keys)?.toString()
        ?: error("Missing required config key: ${keys.joinToString(" or ")}")
}

private fun optionalString(config: Map<String, Any?>, vararg keys: String): String? {
    return firstValue(config, *keys)?.toString()?.takeIf(String::isNotBlank)
}

private fun optionalBoolean(config: Map<String, Any?>, vararg keys: String): Boolean? {
    return firstValue(config, *keys)?.toString()?.toBooleanStrictOrNull()
}

private fun requiredStringList(config: Map<String, Any?>, key: String): List<String> {
    val value = config[key] ?: error("Missing required config key: $key")
    return when (value) {
        is List<*> -> value.map { it?.toString() ?: error("Null value in config key: $key") }
        is String -> value.split(",").map(String::trim).filter(String::isNotEmpty)
        else -> error("Config key $key must be a list or comma-separated string")
    }
}

private fun optionalStringMap(config: Map<String, Any?>, key: String): Map<String, String> {
    val value = config[key] ?: return emptyMap()
    return when (value) {
        is Map<*, *> -> value.mapKeys { (entryKey, _) -> entryKey?.toString() ?: error("Null key in config key: $key") }
            .mapValues { (_, entryValue) -> entryValue?.toString() ?: error("Null value in config key: $key") }
        else -> error("Config key $key must be an object")
    }
}

private fun firstValue(config: Map<String, Any?>, vararg keys: String): Any? {
    return keys.firstNotNullOfOrNull { key -> config[key] }
}

private fun saveMode(value: Any?, default: SaveMode = SaveMode.ErrorIfExists): SaveMode {
    return when (value?.toString()?.lowercase()) {
        null -> default
        "overwrite" -> SaveMode.Overwrite
        "append" -> SaveMode.Append
        "ignore" -> SaveMode.Ignore
        "error", "errorifexists" -> SaveMode.ErrorIfExists
        else -> error("Unsupported save_mode: $value")
    }
}
