package org.openprojectx.spark.boot.connectors

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions
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

class JdbcSourceNode : SparkSourceNode<Dataset<Row>> {
    lateinit var url: String
    lateinit var table: String
    lateinit var user: String
    lateinit var password: String
    var driver: String? = null

    override val name: String = "jdbc-source"

    override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> {
        val reader = context.spark.read()
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
        driver?.let { reader.option("driver", it) }
        return reader.load()
    }
}

class IcebergSinkNode : SparkSinkNode<Dataset<Row>> {
    lateinit var table: String
    var mode: SaveMode = SaveMode.ErrorIfExists

    override val name: String = "iceberg-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val output = input.localCheckpoint(true)
        when (mode) {
            SaveMode.Overwrite -> {
                context.spark.sql("DROP TABLE IF EXISTS $table")
                createTable(output, context)
                output.writeTo(table).append()
            }
            SaveMode.Append -> output.writeTo(table).append()
            SaveMode.Ignore -> if (!tableExists(context)) {
                createTable(output, context)
                output.writeTo(table).append()
            }
            SaveMode.ErrorIfExists -> {
                check(!tableExists(context)) { "Iceberg table already exists: $table" }
                createTable(output, context)
                output.writeTo(table).append()
            }
        }
    }

    private fun createTable(input: Dataset<Row>, context: SparkExecutionContext) {
        context.spark.sql("CREATE TABLE $table (${input.schema().toDDL()}) USING iceberg")
    }

    private fun tableExists(context: SparkExecutionContext): Boolean {
        return runCatching {
            context.spark.table(table).queryExecution().analyzed()
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

class JdbcSinkNode : SparkSinkNode<Dataset<Row>> {
    lateinit var url: String
    lateinit var table: String
    lateinit var user: String
    lateinit var password: String
    var mode: SaveMode = SaveMode.Append

    override val name: String = "jdbc-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        input.write()
            .mode(mode)
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .save()
    }
}
