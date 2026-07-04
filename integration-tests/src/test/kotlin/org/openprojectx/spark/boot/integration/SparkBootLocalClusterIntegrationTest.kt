package org.openprojectx.spark.boot.integration

import java.io.NotSerializableException
import java.nio.file.Files
import java.nio.file.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.openprojectx.spark.boot.connectors.SelectNode
import org.openprojectx.spark.boot.connectors.SqlFilterNode
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.core.ExecutableFlow
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkRuntime
import org.openprojectx.spark.boot.runtime.spark.SparkSinkNode
import org.openprojectx.spark.boot.runtime.spark.SparkSourceNode

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkBootLocalClusterIntegrationTest {
    private lateinit var spark: SparkSession
    private lateinit var runtime: SparkRuntime

    @BeforeAll
    fun startSpark() {
        val sparkHome = sparkHome()
        assumeTrue(
            sparkHome != null,
            "Spark local-cluster tests require SPARK_HOME, spark.home, or spark.test.home pointing at a Spark distribution."
        )
        System.setProperty("spark.test.home", sparkHome.toString())

        spark = SparkSession.builder()
            .appName("spark-boot-local-cluster-test")
            .master("local-cluster[1,1,1024]")
            .config("spark.home", sparkHome.toString())
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.executor.memory", "512m")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.planChangeValidation", "false")
            .config("spark.sql.lightweightPlanChangeValidation", "false")
            .config(
                "spark.executor.extraJavaOptions",
                "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
                    "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                    "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                    "--add-opens=java.base/java.net=ALL-UNNAMED",
            )
            .getOrCreate()
        runtime = SparkRuntime(SparkExecutionContext(spark))
    }

    @AfterAll
    fun stopSpark() {
        if (::spark.isInitialized) {
            spark.stop()
        }
    }

    @Test
    fun `built-in SQL plan nodes run on a local Spark cluster`() {
        val sink = CollectingSinkNode()
        val flow = ExecutableFlow(
            name = "local-cluster-built-ins",
            nodes = mapOf(
                "orders" to LocalOrdersSourceNode(),
                "paid-only" to SqlFilterNode().apply {
                    condition = "status = 'PAID'"
                },
                "select-columns" to SelectNode().apply {
                    columns = listOf("id", "amount", "status")
                },
                "sink" to sink
            ),
            edges = listOf(
                EdgeDefinition("orders", "paid-only"),
                EdgeDefinition("paid-only", "select-columns"),
                EdgeDefinition("select-columns", "sink")
            )
        )

        runtime.run(flow)

        assertEquals(listOf("1"), sink.ids)
    }

    @Test
    fun `executor-side Kotlin closures must not capture non-serializable state`() {
        val sink = UnsafeExecutorClosureSink(NonSerializableFormatter("order-"))
        val flow = ExecutableFlow(
            name = "local-cluster-unsafe-closure",
            nodes = mapOf(
                "orders" to LocalOrdersSourceNode(),
                "sink" to sink
            ),
            edges = listOf(EdgeDefinition("orders", "sink"))
        )

        val error = assertThrows(SparkException::class.java) {
            runtime.run(flow)
        }

        assertEquals(true, error.containsCause<NotSerializableException>())
    }

    private class LocalOrdersSourceNode : SparkSourceNode<Dataset<Row>> {
        override val name: String = "local-orders-source"

        override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> {
            val orders = listOf(
                Order(id = "1", amount = 10.0, status = "PAID"),
                Order(id = "2", amount = 20.0, status = "CANCELLED")
            )
            return context.spark.createDataFrame(orders, Order::class.java)
        }
    }

    private class CollectingSinkNode : SparkSinkNode<Dataset<Row>> {
        override val name: String = "collecting-sink"
        val ids = mutableListOf<String>()

        override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
            ids += input.collectAsList().map { row -> row.getAs<String>("id") }
        }
    }

    private class UnsafeExecutorClosureSink(
        private val formatter: NonSerializableFormatter
    ) : SparkSinkNode<Dataset<Row>> {
        override val name: String = "unsafe-executor-closure-sink"

        override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
            input.javaRDD()
                .map { row: Row -> formatter.format(row.getAs<String>("id")) }
                .saveAsTextFile(tempOutputPath())
        }
    }

    private class NonSerializableFormatter(
        private val prefix: String
    ) {
        fun format(id: String): String = "$prefix$id"
    }

    data class Order(
        val id: String = "",
        val amount: Double = 0.0,
        val status: String = ""
    )
}

private fun sparkHome(): Path? {
    return listOfNotNull(
        System.getProperty("spark.test.home"),
        System.getProperty("spark.home"),
        System.getenv("SPARK_HOME")
    )
        .map(Path::of)
        .firstOrNull { path ->
            Files.isDirectory(path) &&
                (Files.isDirectory(path.resolve("jars")) || Files.isDirectory(path.resolve("assembly/target/scala-2.13/jars")))
        }
}

private fun Throwable.containsCause(type: Class<out Throwable>): Boolean {
    var current: Throwable? = this
    while (current != null) {
        if (type.isInstance(current)) {
            return true
        }
        current = current.cause
    }
    return false
}

private inline fun <reified T : Throwable> Throwable.containsCause(): Boolean {
    return containsCause(T::class.java)
}

private fun tempOutputPath(): String {
    val output = kotlin.io.path.createTempDirectory("spark-boot-unsafe-closure-output")
    java.nio.file.Files.delete(output)
    return output.toString()
}
