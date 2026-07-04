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

class JdbcSourceNodeFactory @Inject constructor(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry
) : NodeFactory<JdbcSourceNode> {
    override fun create(): JdbcSourceNode = JdbcSourceNode(jdbcConnectionRegistry)
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

private fun requiredString(config: Map<String, Any?>, key: String): String {
    return config[key]?.toString() ?: error("Missing required config key: $key")
}

private fun optionalString(config: Map<String, Any?>, key: String): String? {
    return config[key]?.toString()?.takeIf(String::isNotBlank)
}

private fun requiredStringList(config: Map<String, Any?>, key: String): List<String> {
    val value = config[key] ?: error("Missing required config key: $key")
    return when (value) {
        is List<*> -> value.map { it?.toString() ?: error("Null value in config key: $key") }
        is String -> value.split(",").map(String::trim).filter(String::isNotEmpty)
        else -> error("Config key $key must be a list or comma-separated string")
    }
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
