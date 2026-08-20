package org.openprojectx.spark.boot.examples.multihmscatalogs

import org.apache.spark.sql.SparkSession

private const val WAREHOUSE_ROOT = "file:/tmp/spark-boot-multi-hms-catalogs"

fun configureMultiHmsCatalogs() {
    val analyticsUri = requiredProperty(
        "bigdata.test.endpoint.analytics.hive-metastore.properties.hive.metastore.uris",
    )
    val archiveUri = requiredProperty(
        "bigdata.test.endpoint.archive.hive-metastore.properties.hive.metastore.uris",
    )

    configureCatalog("analytics_iceberg", analyticsUri, "$WAREHOUSE_ROOT/analytics/iceberg")
    configureCatalog("archive_iceberg", archiveUri, "$WAREHOUSE_ROOT/archive/iceberg")
    configureCatalog("analytics_hive", analyticsUri, "$WAREHOUSE_ROOT/analytics/hive")
    configureCatalog("archive_hive", archiveUri, "$WAREHOUSE_ROOT/archive/hive")
}

fun runMultiHmsCatalogsExample(spark: SparkSession) {
    check(spark.conf().get("spark.sql.catalogImplementation") == "hive") {
        "Expected Kyuubi Hive catalogs to enable Spark Hive support"
    }

    val suffix = System.nanoTime().toString()

    writeIcebergTable(
        spark = spark,
        catalog = "analytics_iceberg",
        table = "orders_$suffix",
        label = "analytics",
    )
    writeIcebergTable(
        spark = spark,
        catalog = "archive_iceberg",
        table = "orders_$suffix",
        label = "archive",
    )
    assertTableRows(spark, "analytics_iceberg.default.orders_$suffix", "analytics")
    assertTableRows(spark, "archive_iceberg.default.orders_$suffix", "archive")

    writeHiveTable(
        spark = spark,
        catalog = "analytics_hive",
        table = "events_$suffix",
        label = "analytics-hive",
    )
    writeHiveTable(
        spark = spark,
        catalog = "archive_hive",
        table = "events_$suffix",
        label = "archive-hive",
    )
    assertTableRows(spark, "analytics_hive.default.events_$suffix", "analytics-hive")
    assertTableRows(spark, "archive_hive.default.events_$suffix", "archive-hive")
}

private fun configureCatalog(name: String, uri: String, warehouse: String) {
    System.setProperty("spark.boot.catalogs.$name.uri", uri)
    if (name.endsWith("_iceberg")) {
        System.setProperty("spark.boot.catalogs.$name.warehouse", warehouse)
    }
    if (name.endsWith("_hive")) {
        System.setProperty("spark.boot.catalogs.$name.properties.warehouse", warehouse)
    }
}

private fun writeIcebergTable(
    spark: SparkSession,
    catalog: String,
    table: String,
    label: String,
) {
    val identifier = "$catalog.default.$table"
    val location = "$WAREHOUSE_ROOT/$catalog/$table"
    spark.sql("DROP TABLE IF EXISTS $identifier")
    spark.sql("CREATE TABLE $identifier (id BIGINT, source STRING) USING iceberg LOCATION '$location'")
    spark.sql("INSERT INTO $identifier VALUES (1, '$label'), (2, '$label')")
}

private fun writeHiveTable(
    spark: SparkSession,
    catalog: String,
    table: String,
    label: String,
) {
    val identifier = "$catalog.default.$table"
    val location = "$WAREHOUSE_ROOT/$catalog/$table"
    spark.sql("DROP TABLE IF EXISTS $identifier")
    spark.sql("CREATE TABLE $identifier (id BIGINT, source STRING) USING parquet LOCATION '$location'")
    spark.sql("INSERT INTO $identifier VALUES (1, '$label'), (2, '$label')")
}

private fun assertTableRows(spark: SparkSession, identifier: String, label: String) {
    val rows = spark.sql("SELECT id, source FROM $identifier ORDER BY id")
        .collectAsList()
        .map { row -> row.getLong(0) to row.getString(1) }

    check(rows == listOf(1L to label, 2L to label)) {
        "Expected two $label rows in $identifier, got $rows"
    }
}

private fun requiredProperty(name: String): String =
    System.getProperty(name)
        ?: error("Missing system property '$name'. Run with the bigdata-test Gradle plugin.")
