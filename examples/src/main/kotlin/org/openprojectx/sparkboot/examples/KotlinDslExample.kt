package org.openprojectx.sparkboot.examples

import org.apache.spark.sql.SaveMode
import org.openprojectx.sparkboot.dagger.DaggerSparkBootComponent
import org.openprojectx.sparkboot.dsl.kotlin.filterSql
import org.openprojectx.sparkboot.dsl.kotlin.select
import org.openprojectx.sparkboot.dsl.kotlin.sparkFlow
import org.openprojectx.sparkboot.dsl.kotlin.writeParquet

fun main() {
    val component = DaggerSparkBootComponent.create()

    val flow = sparkFlow("paid-orders", component) {
        parquetSource("orders") {
            path = "data/orders"
        }
            .filterSql("paid-only") {
                condition = "status = 'PAID'"
            }
            .select("select-columns") {
                columns = listOf("id", "amount", "status")
            }
            .writeParquet("sink") {
                path = "output/paid-orders"
                mode = SaveMode.Overwrite
            }
    }

    component.sparkRuntime().run(flow)
}
