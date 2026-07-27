package org.openprojectx.spark.boot.examples.kafkahudihms

import com.typesafe.config.ConfigFactory
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dsl.hocon.SeaTunnelStyleConfigParser
import org.openprojectx.spark.boot.dsl.kotlin.sparkFlow
import org.openprojectx.spark.boot.dsl.kotlin.transformDataFrame
import org.openprojectx.spark.boot.dsl.kotlin.writeHudi

const val HUDI_DATABASE = "spark_boot_hudi"
const val S3_BUCKET = "spark-boot-kafka-hudi-hms"

data class KafkaHudiScenario(
    val topic: String,
    val hudiPath: String,
    val hudiTable: String
)

fun newScenario(prefix: String): KafkaHudiScenario {
    val suffix = System.currentTimeMillis().toString()
    return KafkaHudiScenario(
        topic = "spark-boot-$prefix-orders-$suffix",
        hudiPath = "s3a://$S3_BUCKET/hudi/$prefix/orders-$suffix",
        hudiTable = "${prefix}_paid_orders_$suffix".replace('-', '_')
    )
}

fun configureSparkBootEnvironment() {
    System.getProperty("hive.metastore.uris")?.let { metastoreUris ->
        System.setProperty("spark.boot.hms.uri", metastoreUris)
    }
    System.getProperty("spark.boot.hudi.warehouse")?.let { warehouse ->
        System.setProperty("spark.boot.hms.warehouse", warehouse)
    }
}

fun kafkaBootstrapServers(): String {
    return System.getProperty("bootstrap.servers")
        ?: error("Missing bootstrap.servers. Run this example through Gradle so bigdata-test can inject Kafka.")
}

fun seedKafkaOrders(bootstrapServers: String, topic: String) {
    createTopic(bootstrapServers, topic)

    val props = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }

    KafkaProducer<String, String>(props).use { producer ->
        listOf(
            "1" to """{"id":"1","amount":10.0,"status":"PAID","event_ts":1000}""",
            "2" to """{"id":"2","amount":20.0,"status":"CANCELLED","event_ts":1001}""",
            "3" to """{"id":"3","amount":30.0,"status":"PAID","event_ts":1002}"""
        ).forEach { (key, value) ->
            producer.send(ProducerRecord(topic, key, value)).get(30, TimeUnit.SECONDS)
        }
        producer.flush()
    }
}

fun SparkBootComponent.prepareHudiDatabase() {
    sparkSession().sql("CREATE DATABASE IF NOT EXISTS $HUDI_DATABASE LOCATION 's3a://$S3_BUCKET/hive/$HUDI_DATABASE.db'")
}

fun SparkBootComponent.runConfig(configText: String) {
    val definition = SeaTunnelStyleConfigParser().parse(ConfigFactory.parseString(configText))
    val executableFlow = FlowAssembler(nodeFactoryRegistry()).assemble(definition)
    sparkRuntime().run(executableFlow)
}

fun SparkBootComponent.runKotlinKafkaToHudiFlow(
    bootstrapServers: String,
    scenario: KafkaHudiScenario
) {
    val flow = sparkFlow("kafka-to-hudi-hms-kotlin", this) {
        kafkaSource("orders") {
            this.bootstrapServers = bootstrapServers
            topic = scenario.topic
        }.transformDataFrame("paid_orders") { input ->
            input.selectExpr("from_json(value, 'id STRING, amount DOUBLE, status STRING, event_ts LONG') AS parsed")
                .selectExpr(
                    "parsed.id AS id",
                    "parsed.amount AS amount",
                    "parsed.status AS status",
                    "parsed.event_ts AS event_ts"
                )
                .filter("status = 'PAID'")
        }.writeHudi("hudi_sink") {
            path = scenario.hudiPath
            table = scenario.hudiTable
            database = HUDI_DATABASE
            recordKeyField = "id"
            precombineField = "event_ts"
            mode = SaveMode.Overwrite
            hiveSync = true
        }
    }

    sparkRuntime().run(flow)
}

fun kafkaToHudiConfig(bootstrapServers: String, scenario: KafkaHudiScenario): String =
    """
    env {
      job.name = "kafka-to-hudi-hms"
    }

    source = [
      {
        plugin_name = "Kafka"
        plugin_output = "kafka_orders"
        bootstrap_servers = "$bootstrapServers"
        topic = "${scenario.topic}"
        starting_offsets = "earliest"
        ending_offsets = "latest"
      }
    ]

    transform = [
      {
        plugin_name = "Sql"
        plugin_input = "kafka_orders"
        plugin_output = "paid_orders"
        query = ${"\"\"\""}
          SELECT
            parsed.id AS id,
            parsed.amount AS amount,
            parsed.status AS status,
            parsed.event_ts AS event_ts
          FROM (
            SELECT from_json(value, 'id STRING, amount DOUBLE, status STRING, event_ts LONG') AS parsed
            FROM kafka_orders
          )
          WHERE parsed.status = 'PAID'
        ${"\"\"\""}
      }
    ]

    sink = [
      {
        plugin_name = "Hudi"
        plugin_input = "paid_orders"
        path = "${scenario.hudiPath}"
        table = "${scenario.hudiTable}"
        database = "$HUDI_DATABASE"
        record_key_field = "id"
        precombine_field = "event_ts"
        save_mode = "overwrite"
        hive_sync = true
      }
    ]
    """.trimIndent()

fun assertHudiRows(spark: SparkSession, scenario: KafkaHudiScenario) {
    val values = spark.read()
        .format("hudi")
        .load(scenario.hudiPath)
        .select("id", "amount", "status")
        .collectAsList()
        .map { row -> Triple(row.getString(0), row.getDouble(1), row.getString(2)) }

    check(values.size == 2) { "Expected two paid Hudi rows in $values" }
    check(values.any { it.first == "1" && it.third == "PAID" }) {
        "Expected paid order 1 in $values"
    }
    check(values.any { it.first == "3" && it.third == "PAID" }) {
        "Expected paid order 3 in $values"
    }
    check(values.none { it.first == "2" }) {
        "Cancelled order should not be written to Hudi: $values"
    }
}

fun assertHmsTable(spark: SparkSession, scenario: KafkaHudiScenario) {
    val count = spark.sql("SHOW TABLES IN $HUDI_DATABASE LIKE '${scenario.hudiTable}'").count()
    check(count == 1L) { "Expected HMS table $HUDI_DATABASE.${scenario.hudiTable}" }
}

private fun createTopic(bootstrapServers: String, topic: String) {
    val props = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
    }
    AdminClient.create(props).use { admin ->
        try {
            admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get(30, TimeUnit.SECONDS)
        } catch (error: Exception) {
            if (error.cause !is TopicExistsException) {
                throw error
            }
        }
    }
}
