package org.openprojectx.spark.boot.connectors

import org.openprojectx.spark.boot.core.ConfigFieldDescriptor
import org.openprojectx.spark.boot.core.ConfigFieldType
import org.openprojectx.spark.boot.core.NodeDescriptor
import org.openprojectx.spark.boot.core.NodeRole

object BuiltinNodeDescriptors {
    val all: List<NodeDescriptor> = listOf(
        NodeDescriptor(
            type = "ParquetSource",
            label = "Parquet Source",
            role = NodeRole.SOURCE,
            category = "Source",
            description = "Reads a Spark DataFrame from a Parquet path.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "path",
                    label = "Path",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Input path, for example file:/data/orders or s3a://bucket/orders."
                )
            )
        ),
        NodeDescriptor(
            type = "JdbcSource",
            label = "JDBC Source",
            role = NodeRole.SOURCE,
            category = "Source",
            description = "Reads a Spark DataFrame from a JDBC table or query.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "connection",
                    label = "Connection",
                    type = ConfigFieldType.STRING,
                    description = "Logical connection name from spark.boot.jdbc.connections."
                ),
                ConfigFieldDescriptor(
                    key = "url",
                    label = "URL",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC URL. Overrides the named connection URL."
                ),
                ConfigFieldDescriptor(
                    key = "table",
                    label = "Table",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "JDBC dbtable value."
                ),
                ConfigFieldDescriptor(
                    key = "user",
                    label = "User",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC user. Overrides the named connection user."
                ),
                ConfigFieldDescriptor(
                    key = "password",
                    label = "Password",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC password. Overrides the named connection password.",
                    secret = true
                ),
                ConfigFieldDescriptor(
                    key = "driver",
                    label = "Driver",
                    type = ConfigFieldType.STRING,
                    description = "Optional JDBC driver class."
                )
            )
        ),
        NodeDescriptor(
            type = "KafkaSource",
            label = "Kafka Source",
            role = NodeRole.SOURCE,
            category = "Source",
            description = "Reads a bounded Spark DataFrame from Kafka and casts key/value to strings.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "bootstrap_servers",
                    label = "Bootstrap Servers",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Kafka bootstrap servers."
                ),
                ConfigFieldDescriptor(
                    key = "topic",
                    label = "Topic",
                    type = ConfigFieldType.STRING,
                    description = "Single topic to subscribe to. Use subscribe, subscribe_pattern, or assign for advanced cases."
                ),
                ConfigFieldDescriptor(
                    key = "subscribe",
                    label = "Subscribe",
                    type = ConfigFieldType.STRING,
                    description = "Comma-separated topic subscription."
                ),
                ConfigFieldDescriptor(
                    key = "subscribe_pattern",
                    label = "Subscribe Pattern",
                    type = ConfigFieldType.STRING,
                    description = "Kafka topic subscription regex."
                ),
                ConfigFieldDescriptor(
                    key = "assign",
                    label = "Assign",
                    type = ConfigFieldType.STRING,
                    description = "Explicit Kafka partition assignment JSON."
                ),
                ConfigFieldDescriptor(
                    key = "starting_offsets",
                    label = "Starting Offsets",
                    type = ConfigFieldType.STRING,
                    defaultValue = "earliest",
                    description = "Spark Kafka startingOffsets value."
                ),
                ConfigFieldDescriptor(
                    key = "ending_offsets",
                    label = "Ending Offsets",
                    type = ConfigFieldType.STRING,
                    defaultValue = "latest",
                    description = "Spark Kafka endingOffsets value for bounded reads."
                )
            )
        ),
        NodeDescriptor(
            type = "HudiSource",
            label = "Hudi Source",
            role = NodeRole.SOURCE,
            category = "Source",
            description = "Reads a Spark DataFrame from a Hudi table path.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "path",
                    label = "Path",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Hudi table path, for example s3a://bucket/tables/orders."
                ),
                ConfigFieldDescriptor(
                    key = "query_type",
                    label = "Query Type",
                    type = ConfigFieldType.STRING,
                    description = "Optional Hudi query type, such as snapshot or read_optimized."
                )
            )
        ),
        NodeDescriptor(
            type = "SqlFilterTransform",
            label = "SQL Filter",
            role = NodeRole.TRANSFORM,
            category = "Transform",
            description = "Filters rows with a Spark SQL boolean expression.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "condition",
                    label = "Condition",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Spark SQL filter condition, for example status = 'PAID'."
                )
            )
        ),
        NodeDescriptor(
            type = "SelectTransform",
            label = "Select Columns",
            role = NodeRole.TRANSFORM,
            category = "Transform",
            description = "Selects one or more columns from the upstream DataFrame.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "columns",
                    label = "Columns",
                    type = ConfigFieldType.STRING_LIST,
                    required = true,
                    description = "Column names as a list or comma-separated string."
                )
            )
        ),
        NodeDescriptor(
            type = "SqlTransform",
            label = "SQL Transform",
            role = NodeRole.TRANSFORM,
            category = "Transform",
            description = "Registers the upstream DataFrame as a temp view and runs a Spark SQL query.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "plugin_input",
                    label = "Input View",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Temp view name assigned to the upstream DataFrame."
                ),
                ConfigFieldDescriptor(
                    key = "plugin_output",
                    label = "Output View",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Temp view name assigned to the SQL query result."
                ),
                ConfigFieldDescriptor(
                    key = "query",
                    label = "Query",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Spark SQL query."
                )
            )
        ),
        NodeDescriptor(
            type = "ParquetSink",
            label = "Parquet Sink",
            role = NodeRole.SINK,
            category = "Sink",
            description = "Writes the upstream DataFrame to Parquet.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "path",
                    label = "Path",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Output path, for example file:/data/output or s3a://bucket/output."
                ),
                saveModeField(defaultValue = "errorifexists")
            )
        ),
        NodeDescriptor(
            type = "JdbcSink",
            label = "JDBC Sink",
            role = NodeRole.SINK,
            category = "Sink",
            description = "Writes the upstream DataFrame to a JDBC table.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "connection",
                    label = "Connection",
                    type = ConfigFieldType.STRING,
                    description = "Logical connection name from spark.boot.jdbc.connections."
                ),
                ConfigFieldDescriptor(
                    key = "url",
                    label = "URL",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC URL. Overrides the named connection URL."
                ),
                ConfigFieldDescriptor(
                    key = "table",
                    label = "Table",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Target JDBC table."
                ),
                ConfigFieldDescriptor(
                    key = "user",
                    label = "User",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC user. Overrides the named connection user."
                ),
                ConfigFieldDescriptor(
                    key = "password",
                    label = "Password",
                    type = ConfigFieldType.STRING,
                    description = "Explicit JDBC password. Overrides the named connection password.",
                    secret = true
                ),
                saveModeField(defaultValue = "append")
            )
        ),
        NodeDescriptor(
            type = "HudiSink",
            label = "Hudi Sink",
            role = NodeRole.SINK,
            category = "Sink",
            description = "Writes the upstream DataFrame to a Hudi table path.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "path",
                    label = "Path",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Hudi table output path, for example s3a://bucket/tables/orders."
                ),
                ConfigFieldDescriptor(
                    key = "table",
                    label = "Table",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Hudi table name."
                ),
                ConfigFieldDescriptor(
                    key = "database",
                    label = "Database",
                    type = ConfigFieldType.STRING,
                    defaultValue = "default",
                    description = "Logical database name used for HMS sync."
                ),
                ConfigFieldDescriptor(
                    key = "record_key_field",
                    label = "Record Key Field",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Hudi record key field."
                ),
                ConfigFieldDescriptor(
                    key = "precombine_field",
                    label = "Precombine Field",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Hudi precombine field."
                ),
                ConfigFieldDescriptor(
                    key = "partition_path_field",
                    label = "Partition Path Field",
                    type = ConfigFieldType.STRING,
                    description = "Optional Hudi partition field."
                ),
                ConfigFieldDescriptor(
                    key = "hive_sync",
                    label = "Hive Sync",
                    type = ConfigFieldType.BOOLEAN,
                    defaultValue = "false",
                    description = "Enable Hudi Hive Metastore sync."
                ),
                saveModeField(defaultValue = "errorifexists")
            )
        ),
        NodeDescriptor(
            type = "IcebergSink",
            label = "Iceberg Sink",
            role = NodeRole.SINK,
            category = "Sink",
            description = "Writes the upstream DataFrame to an Iceberg table.",
            config = listOf(
                ConfigFieldDescriptor(
                    key = "catalog",
                    label = "Catalog",
                    type = ConfigFieldType.STRING,
                    description = "Optional logical Iceberg catalog name."
                ),
                ConfigFieldDescriptor(
                    key = "table",
                    label = "Table",
                    type = ConfigFieldType.STRING,
                    required = true,
                    description = "Target table, optionally catalog-qualified."
                ),
                saveModeField(defaultValue = "errorifexists")
            )
        )
    )

    private fun saveModeField(defaultValue: String): ConfigFieldDescriptor {
        return ConfigFieldDescriptor(
            key = "mode",
            label = "Save Mode",
            type = ConfigFieldType.SAVE_MODE,
            description = "Spark save mode.",
            defaultValue = defaultValue,
            options = listOf("overwrite", "append", "ignore", "errorifexists")
        )
    }
}
