package org.openprojectx.spark.boot.examples.app

import javax.inject.Inject
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.openprojectx.spark.boot.core.NodeFactory
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSourceNode

class InMemoryOrdersSourceNode : SparkSourceNode<Dataset<Row>> {
    var orders: List<Order> = emptyList()

    override val name: String = "in-memory-orders-source"

    override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> {
        return context.spark.createDataFrame(orders, Order::class.java)
    }
}

class InMemoryOrdersSourceNodeFactory @Inject constructor() : NodeFactory<InMemoryOrdersSourceNode> {
    override fun create(): InMemoryOrdersSourceNode {
        return InMemoryOrdersSourceNode()
    }
}
