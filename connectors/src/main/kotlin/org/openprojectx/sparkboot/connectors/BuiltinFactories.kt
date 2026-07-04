package org.openprojectx.sparkboot.connectors

import javax.inject.Inject
import org.apache.spark.sql.SaveMode
import org.openprojectx.sparkboot.core.ConfigNodeFactory
import org.openprojectx.sparkboot.core.FlowNode
import org.openprojectx.sparkboot.core.NodeFactory

class ParquetSourceNodeFactory @Inject constructor() : NodeFactory<ParquetSourceNode> {
    override fun create(): ParquetSourceNode = ParquetSourceNode()
}

class ParquetSinkNodeFactory @Inject constructor() : NodeFactory<ParquetSinkNode> {
    override fun create(): ParquetSinkNode = ParquetSinkNode()
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

class JdbcSinkNodeFactory @Inject constructor() : NodeFactory<JdbcSinkNode> {
    override fun create(): JdbcSinkNode = JdbcSinkNode()
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

class JdbcSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return JdbcSinkNode().apply {
            url = requiredString(config, "url")
            table = requiredString(config, "table")
            user = requiredString(config, "user")
            password = requiredString(config, "password")
            mode = saveMode(config["save_mode"] ?: config["mode"], SaveMode.Append)
        }
    }
}

private fun requiredString(config: Map<String, Any?>, key: String): String {
    return config[key]?.toString() ?: error("Missing required config key: $key")
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
