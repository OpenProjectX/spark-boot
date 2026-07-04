package org.openprojectx.spark.boot.examples.app

import org.apache.spark.sql.SaveMode
import org.openprojectx.spark.boot.dsl.kotlin.SparkBootContext
import org.openprojectx.spark.boot.dsl.kotlin.filterSql
import org.openprojectx.spark.boot.dsl.kotlin.select
import org.openprojectx.spark.boot.dsl.kotlin.writeParquet

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

fun SparkBootContext.profiledPaidOrdersFlow(output: String) = flow("profiled-paid-orders-app") {
    node<InMemoryOrdersSourceNode>("orders", "ProfiledOrdersSource") {
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
