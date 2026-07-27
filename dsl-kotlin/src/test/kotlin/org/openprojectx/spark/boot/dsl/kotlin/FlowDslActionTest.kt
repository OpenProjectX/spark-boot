package org.openprojectx.spark.boot.dsl.kotlin

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.dagger.DaggerSparkBootComponent

class FlowDslActionTest {
    @Test
    fun `thenSqlAction off sink creates control edge`() {
        val flow = sparkFlow("orders", component) {
            parquetSource("orders") {
                path = "data/orders"
            }.writeParquet("sink") {
                path = "data/output"
                mode = SaveMode.Overwrite
            }.thenSqlAction("repair") {
                sql = "MSCK REPAIR TABLE db.orders"
            }
        }

        assertEquals(setOf("orders", "sink", "repair"), flow.nodes.keys)
        assertEquals(
            listOf(
                EdgeDefinition("orders", "sink"),
                EdgeDefinition("sink", "repair")
            ),
            flow.edges
        )
    }

    @Test
    fun `standalone sqlAction builds one-node flow`() {
        val flow = sparkFlow("init", component) {
            sqlAction("init") {
                sql = "CREATE DATABASE IF NOT EXISTS mart"
            }
        }

        assertEquals(setOf("init"), flow.nodes.keys)
        assertEquals(emptyList<EdgeDefinition>(), flow.edges)
    }

    companion object {
        private val component = DaggerSparkBootComponent.create()
    }
}
