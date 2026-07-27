package org.openprojectx.spark.boot.dsl.hocon

import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.openprojectx.spark.boot.core.EdgeDefinition

class SeaTunnelStyleConfigParserActionTest {
    private val parser = SeaTunnelStyleConfigParser()

    @Test
    fun `parses standalone action object form`() {
        val flow = parser.parse(
            ConfigFactory.parseString(
                """
                env { job.name = "expire-only" }
                action {
                  Sql {
                    sql = "CALL cat.system.expire_snapshots(table => 'db.orders')"
                  }
                }
                """.trimIndent()
            )
        )

        assertEquals("expire-only", flow.name)
        assertEquals("action_Sql_0", flow.nodes.single().id)
        assertEquals("SqlAction", flow.nodes.single().type)
        assertTrue(flow.edges.isEmpty())
    }

    @Test
    fun `parses action list form with string input and output`() {
        val flow = parser.parse(
            ConfigFactory.parseString(
                """
                action = [
                  {
                    plugin_name = "Sql"
                    plugin_input = "sink_orders_0"
                    plugin_output = "expire"
                    sql = "MSCK REPAIR TABLE db.orders"
                  }
                ]
                """.trimIndent()
            )
        )

        assertEquals("expire", flow.nodes.single().id)
        assertEquals(listOf(EdgeDefinition("sink_orders_0", "expire")), flow.edges)
    }

    @Test
    fun `parses action list form with multiple inputs`() {
        val flow = parser.parse(
            ConfigFactory.parseString(
                """
                action = [
                  {
                    plugin_name = "Sql"
                    plugin_input = ["sink_a", "sink_b"]
                    sql = "CREATE DATABASE IF NOT EXISTS mart"
                  }
                ]
                """.trimIndent()
            )
        )

        assertEquals("action_Sql_0", flow.nodes.single().id)
        assertEquals(
            listOf(
                EdgeDefinition("sink_a", "action_Sql_0"),
                EdgeDefinition("sink_b", "action_Sql_0")
            ),
            flow.edges
        )
    }
}
