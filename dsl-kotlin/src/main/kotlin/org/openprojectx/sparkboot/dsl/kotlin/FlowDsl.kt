package org.openprojectx.sparkboot.dsl.kotlin

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.openprojectx.sparkboot.connectors.ParquetSinkNode
import org.openprojectx.sparkboot.connectors.ParquetSourceNode
import org.openprojectx.sparkboot.connectors.SelectNode
import org.openprojectx.sparkboot.connectors.SqlFilterNode
import org.openprojectx.sparkboot.core.EdgeDefinition
import org.openprojectx.sparkboot.core.ExecutableFlow
import org.openprojectx.sparkboot.core.FlowNode
import org.openprojectx.sparkboot.dagger.SparkBootComponent
import org.openprojectx.sparkboot.runtime.spark.SparkExecutionContext
import org.openprojectx.sparkboot.runtime.spark.SparkTransformNode

open class FlowBuilder {
    private val nodes = mutableMapOf<String, FlowNode<*, *>>()
    private val edges = mutableListOf<EdgeDefinition>()

    fun <T : FlowNode<*, *>> register(id: String, node: T): NodeRef<T> {
        require(id !in nodes) { "Duplicate node id: $id" }
        nodes[id] = node
        return NodeRef(id, node, this)
    }

    fun addEdge(from: String, to: String) {
        edges += EdgeDefinition(from, to)
    }

    fun build(name: String): ExecutableFlow {
        return ExecutableFlow(
            name = name,
            nodes = nodes.toMap(),
            edges = edges.toList()
        )
    }
}

class NodeRef<T : FlowNode<*, *>>(
    val id: String,
    val node: T,
    internal val builder: FlowBuilder
) {
    fun then(next: NodeRef<*>): NodeRef<*> {
        builder.addEdge(id, next.id)
        return next
    }

    fun to(next: NodeRef<*>) {
        builder.addEdge(id, next.id)
    }
}

fun flow(name: String, block: FlowBuilder.() -> Unit): ExecutableFlow {
    val builder = FlowBuilder()
    builder.block()
    return builder.build(name)
}

class SparkFlowBuilder(
    private val component: SparkBootComponent
) : FlowBuilder() {
    fun parquetSource(id: String, customize: ParquetSourceNode.() -> Unit): NodeRef<ParquetSourceNode> {
        val node = component.parquetSourceNodeFactory().create()
        node.customize()
        return register(id, node)
    }

    fun sqlFilter(id: String, customize: SqlFilterNode.() -> Unit): NodeRef<SqlFilterNode> {
        val node = component.sqlFilterNodeFactory().create()
        node.customize()
        return register(id, node)
    }

    fun select(id: String, customize: SelectNode.() -> Unit): NodeRef<SelectNode> {
        val node = component.selectNodeFactory().create()
        node.customize()
        return register(id, node)
    }

    fun parquetSink(id: String, customize: ParquetSinkNode.() -> Unit): NodeRef<ParquetSinkNode> {
        val node = component.parquetSinkNodeFactory().create()
        node.customize()
        return register(id, node)
    }

    fun transformDataFrame(
        id: String,
        transform: (Dataset<Row>) -> Dataset<Row>
    ): NodeRef<DataFrameTransformNode> {
        return register(id, DataFrameTransformNode(id, transform))
    }
}

fun sparkFlow(
    name: String,
    component: SparkBootComponent,
    block: SparkFlowBuilder.() -> Unit
): ExecutableFlow {
    val builder = SparkFlowBuilder(component)
    builder.block()
    return builder.build(name)
}

fun NodeRef<*>.filterSql(id: String, customize: SqlFilterNode.() -> Unit): NodeRef<SqlFilterNode> {
    val next = sparkBuilder().sqlFilter(id, customize)
    then(next)
    return next
}

fun NodeRef<*>.select(id: String, customize: SelectNode.() -> Unit): NodeRef<SelectNode> {
    val next = sparkBuilder().select(id, customize)
    then(next)
    return next
}

fun NodeRef<*>.writeParquet(id: String, customize: ParquetSinkNode.() -> Unit): NodeRef<ParquetSinkNode> {
    val next = sparkBuilder().parquetSink(id, customize)
    then(next)
    return next
}

fun NodeRef<*>.transformDataFrame(
    id: String,
    transform: (Dataset<Row>) -> Dataset<Row>
): NodeRef<DataFrameTransformNode> {
    val next = sparkBuilder().transformDataFrame(id, transform)
    then(next)
    return next
}

private fun NodeRef<*>.sparkBuilder(): SparkFlowBuilder {
    return builder as? SparkFlowBuilder
        ?: error("Spark DSL chaining requires SparkFlowBuilder")
}

class DataFrameTransformNode(
    override val name: String,
    private val transform: (Dataset<Row>) -> Dataset<Row>
) : SparkTransformNode<Dataset<Row>, Dataset<Row>> {
    override fun execute(input: Dataset<Row>, context: SparkExecutionContext): Dataset<Row> {
        return transform(input)
    }
}
