package org.openprojectx.spark.boot.examples.app

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import java.nio.file.Files
import javax.inject.Inject
import javax.inject.Singleton
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.openprojectx.spark.boot.core.NodeFactory
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.boot.dagger.BuiltinConnectorModule
import org.openprojectx.spark.boot.dagger.RuntimeModule
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dagger.SparkModule
import org.openprojectx.spark.boot.dsl.kotlin.SparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.SparkBootContext
import org.openprojectx.spark.boot.dsl.kotlin.filterSql
import org.openprojectx.spark.boot.dsl.kotlin.runSparkBoot
import org.openprojectx.spark.boot.dsl.kotlin.select
import org.openprojectx.spark.boot.dsl.kotlin.writeParquet
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSourceNode

@SparkBoot
fun main(args: Array<String>) = runSparkBoot(args, DaggerSparkBootAppComponent.create()) {
    val output = Files.createTempDirectory("spark-boot-app-output")
    Files.delete(output)

    paidOrdersFlow(
        listOf(
            Order(id = "1", amount = 10.0, status = "PAID"),
            Order(id = "2", amount = 20.0, status = "CANCELLED")
        ),
        output.toString()
    )
}

fun SparkBootContext.paidOrdersFlow(
    orders: List<Order>,
    output: String
) = flow("paid-orders-app") {
    node<InMemoryOrdersSourceNode>("orders", "InMemoryOrdersSource") {
        this.orders = orders
    }
        .filterSql("paid-only") {
            condition = "status = 'PAID'"
        }
        .select("select-columns") {
            columns = listOf("id", "amount", "status")
        }
        .writeParquet("sink") {
            path = output
            mode = SaveMode.Overwrite
        }
}

data class Order(
    val id: String = "",
    val amount: Double = 0.0,
    val status: String = ""
)

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

@Module
interface SparkBootAppModule {
    @Binds
    @IntoMap
    @StringKey("InMemoryOrdersSource")
    fun bindInMemoryOrdersSourceFactory(factory: InMemoryOrdersSourceNodeFactory): UntypedNodeFactory
}

@Singleton
@Component(
    modules = [
        SparkModule::class,
        RuntimeModule::class,
        BuiltinConnectorModule::class,
        SparkBootAppModule::class
    ]
)
interface SparkBootAppComponent : SparkBootComponent
