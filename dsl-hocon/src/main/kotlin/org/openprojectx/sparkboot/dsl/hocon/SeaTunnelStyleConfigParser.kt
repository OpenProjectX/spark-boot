package org.openprojectx.sparkboot.dsl.hocon

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueType
import org.openprojectx.sparkboot.core.EdgeDefinition
import org.openprojectx.sparkboot.core.FlowDefinition
import org.openprojectx.sparkboot.core.NodeDefinition

class SeaTunnelStyleConfigParser {
    fun parse(config: Config): FlowDefinition {
        val nodes = mutableListOf<NodeDefinition>()
        val edges = mutableListOf<EdgeDefinition>()

        parseSources(config, nodes)
        parseTransforms(config, nodes, edges)
        parseSinks(config, nodes, edges)

        return FlowDefinition(
            name = readJobName(config),
            nodes = nodes,
            edges = edges,
            config = readEnv(config)
        )
    }

    private fun readJobName(config: Config): String {
        return if (config.hasPath("env.job.name")) {
            config.getString("env.job.name")
        } else {
            "spark-boot-job"
        }
    }

    private fun readEnv(config: Config): Map<String, Any?> {
        return if (config.hasPath("env")) {
            config.getConfig("env").root().unwrapped()
        } else {
            emptyMap()
        }
    }

    private fun parseSources(config: Config, nodes: MutableList<NodeDefinition>) {
        if (!config.hasPath("source")) return

        when (config.getValue("source").valueType()) {
            ConfigValueType.LIST -> config.getConfigList("source").forEach { item ->
                val pluginName = item.getString("plugin_name")
                val output = readOutput(item)
                nodes += NodeDefinition(output, "${pluginName}Source", normalizedConfig(item))
            }
            ConfigValueType.OBJECT -> config.getConfig("source").root().forEach { (pluginName, _) ->
                val item = config.getConfig("source").getConfig(pluginName)
                val output = readOutput(item)
                nodes += NodeDefinition(output, "${pluginName}Source", normalizedConfig(item) + ("plugin_name" to pluginName))
            }
            else -> error("source must be object or list")
        }
    }

    private fun parseTransforms(
        config: Config,
        nodes: MutableList<NodeDefinition>,
        edges: MutableList<EdgeDefinition>
    ) {
        if (!config.hasPath("transform")) return

        when (config.getValue("transform").valueType()) {
            ConfigValueType.LIST -> config.getConfigList("transform").forEach { item ->
                addTransform(item.getString("plugin_name"), item, nodes, edges)
            }
            ConfigValueType.OBJECT -> config.getConfig("transform").root().forEach { (pluginName, _) ->
                addTransform(pluginName, config.getConfig("transform").getConfig(pluginName), nodes, edges)
            }
            else -> error("transform must be object or list")
        }
    }

    private fun addTransform(
        pluginName: String,
        item: Config,
        nodes: MutableList<NodeDefinition>,
        edges: MutableList<EdgeDefinition>
    ) {
        val input = item.getString("plugin_input")
        val output = readOutput(item)
        val nodeConfig = normalizedConfig(item) + mapOf(
            "plugin_name" to pluginName,
            "plugin_output" to output
        )

        nodes += NodeDefinition(output, "${pluginName}Transform", nodeConfig)
        edges += EdgeDefinition(input, output)
    }

    private fun parseSinks(
        config: Config,
        nodes: MutableList<NodeDefinition>,
        edges: MutableList<EdgeDefinition>
    ) {
        if (!config.hasPath("sink")) return

        when (config.getValue("sink").valueType()) {
            ConfigValueType.LIST -> config.getConfigList("sink").forEachIndexed { index, item ->
                addSink(item.getString("plugin_name"), item, index, nodes, edges)
            }
            ConfigValueType.OBJECT -> {
                var index = 0
                config.getConfig("sink").root().forEach { (pluginName, _) ->
                    addSink(pluginName, config.getConfig("sink").getConfig(pluginName), index++, nodes, edges)
                }
            }
            else -> error("sink must be object or list")
        }
    }

    private fun addSink(
        pluginName: String,
        item: Config,
        index: Int,
        nodes: MutableList<NodeDefinition>,
        edges: MutableList<EdgeDefinition>
    ) {
        val input = item.getString("plugin_input")
        val sinkId = "sink_${input}_$index"

        nodes += NodeDefinition(sinkId, "${pluginName}Sink", normalizedConfig(item) + ("plugin_name" to pluginName))
        edges += EdgeDefinition(input, sinkId)
    }

    private fun readOutput(config: Config): String {
        return when {
            config.hasPath("plugin_output") -> config.getString("plugin_output")
            config.hasPath("result_table_name") -> config.getString("result_table_name")
            config.hasPath("source_table_name") -> config.getString("source_table_name")
            else -> error("Missing plugin_output")
        }
    }

    private fun normalizedConfig(config: Config): Map<String, Any?> {
        val values = config.root().unwrapped().toMutableMap()
        if (!values.containsKey("plugin_output") && values.containsKey("result_table_name")) {
            values["plugin_output"] = values["result_table_name"]
        }
        if (!values.containsKey("plugin_output") && values.containsKey("source_table_name")) {
            values["plugin_output"] = values["source_table_name"]
        }
        return values
    }
}
