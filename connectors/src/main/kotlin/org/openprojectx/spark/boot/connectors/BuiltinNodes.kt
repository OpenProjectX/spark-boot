package org.openprojectx.spark.boot.connectors

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions
import org.openprojectx.spark.boot.autoconfigure.IcebergCatalogRegistry
import org.openprojectx.spark.boot.autoconfigure.JdbcConnectionRegistry
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSinkNode
import org.openprojectx.spark.boot.runtime.spark.SparkSourceNode
import org.openprojectx.spark.boot.runtime.spark.SparkTransformNode

class ParquetSourceNode : SparkSourceNode<Dataset<Row>> {
    lateinit var path: String

    override val name: String = "parquet-source"

    override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> {
        return context.spark.read().parquet(path)
    }
}

class ParquetSinkNode : SparkSinkNode<Dataset<Row>> {
    lateinit var path: String
    var mode: SaveMode = SaveMode.ErrorIfExists

    override val name: String = "parquet-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        input.write()
            .mode(mode)
            .parquet(path)
    }
}

class JdbcSourceNode(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry? = null
) : SparkSourceNode<Dataset<Row>> {
    var connection: String? = null
    var url: String? = null
    lateinit var table: String
    var user: String? = null
    var password: String? = null
    var driver: String? = null

    override val name: String = "jdbc-source"

    override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> {
        val configured = connection?.let { connectionName ->
            jdbcConnectionRegistry?.get(connectionName)
                ?: error("JDBC connection '$connectionName' requires JdbcConnectionRegistry")
        }
        val jdbcUrl = url ?: configured?.url ?: error("Missing JDBC url")
        val jdbcUser = user ?: configured?.user ?: error("Missing JDBC user")
        val jdbcPassword = password ?: configured?.password ?: error("Missing JDBC password")
        val jdbcDriver = driver ?: configured?.driver

        val reader = context.spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", table)
            .option("user", jdbcUser)
            .option("password", jdbcPassword)
        jdbcDriver?.let { reader.option("driver", it) }
        return reader.load()
    }
}

class IcebergSinkNode(
    private val icebergCatalogRegistry: IcebergCatalogRegistry? = null
) : SparkSinkNode<Dataset<Row>> {
    var catalog: String? = null
    lateinit var table: String
    var mode: SaveMode = SaveMode.ErrorIfExists

    override val name: String = "iceberg-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val resolvedTable = resolvedTable()
        val output = input.localCheckpoint(true)
        when (mode) {
            SaveMode.Overwrite -> {
                context.spark.sql("DROP TABLE IF EXISTS $resolvedTable")
                createTable(output, context, resolvedTable)
                output.writeTo(resolvedTable).append()
            }
            SaveMode.Append -> output.writeTo(resolvedTable).append()
            SaveMode.Ignore -> if (!tableExists(context, resolvedTable)) {
                createTable(output, context, resolvedTable)
                output.writeTo(resolvedTable).append()
            }
            SaveMode.ErrorIfExists -> {
                check(!tableExists(context, resolvedTable)) { "Iceberg table already exists: $resolvedTable" }
                createTable(output, context, resolvedTable)
                output.writeTo(resolvedTable).append()
            }
        }
    }

    private fun resolvedTable(): String {
        val catalogName = catalog ?: return table
        icebergCatalogRegistry?.get(catalogName)
            ?: error("Iceberg catalog '$catalogName' requires IcebergCatalogRegistry")
        return if (table == catalogName || table.startsWith("$catalogName.")) {
            table
        } else {
            "$catalogName.$table"
        }
    }

    private fun createTable(input: Dataset<Row>, context: SparkExecutionContext, resolvedTable: String) {
        context.spark.sql("CREATE TABLE $resolvedTable (${input.schema().toDDL()}) USING iceberg")
    }

    private fun tableExists(context: SparkExecutionContext, resolvedTable: String): Boolean {
        return runCatching {
            context.spark.table(resolvedTable).queryExecution().analyzed()
            true
        }.getOrDefault(false)
    }
}

class SqlFilterNode : SparkTransformNode<Dataset<Row>, Dataset<Row>> {
    lateinit var condition: String

    override val name: String = "sql-filter"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext): Dataset<Row> {
        return input.filter(condition)
    }
}

class SelectNode : SparkTransformNode<Dataset<Row>, Dataset<Row>> {
    lateinit var columns: List<String>

    override val name: String = "select"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext): Dataset<Row> {
        return input.select(*columns.map { functions.col(it) }.toTypedArray())
    }
}

class SqlTransformNode : SparkTransformNode<Dataset<Row>, Dataset<Row>> {
    lateinit var pluginInput: String
    lateinit var pluginOutput: String
    lateinit var query: String

    override val name: String = "sql-transform"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext): Dataset<Row> {
        input.createOrReplaceTempView(pluginInput)
        val result = context.spark.sql(query)
        result.createOrReplaceTempView(pluginOutput)
        return result
    }
}

class JdbcSinkNode(
    private val jdbcConnectionRegistry: JdbcConnectionRegistry? = null
) : SparkSinkNode<Dataset<Row>> {
    var connection: String? = null
    var url: String? = null
    lateinit var table: String
    var user: String? = null
    var password: String? = null
    var mode: SaveMode = SaveMode.Append

    override val name: String = "jdbc-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val configured = connection?.let { connectionName ->
            jdbcConnectionRegistry?.get(connectionName)
                ?: error("JDBC connection '$connectionName' requires JdbcConnectionRegistry")
        }
        val jdbcUrl = url ?: configured?.url ?: error("Missing JDBC url")
        val jdbcUser = user ?: configured?.user ?: error("Missing JDBC user")
        val jdbcPassword = password ?: configured?.password ?: error("Missing JDBC password")

        input.write()
            .mode(mode)
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", table)
            .option("user", jdbcUser)
            .option("password", jdbcPassword)
            .save()
    }
}
