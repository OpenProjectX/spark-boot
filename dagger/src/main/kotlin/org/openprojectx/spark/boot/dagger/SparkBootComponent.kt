package org.openprojectx.spark.boot.dagger

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.Provides
import dagger.multibindings.IntoMap
import dagger.multibindings.Multibinds
import dagger.multibindings.StringKey
import javax.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.autoconfigure.HmsProperties
import org.openprojectx.spark.boot.autoconfigure.IcebergCatalogProperties
import org.openprojectx.spark.boot.autoconfigure.IcebergCatalogRegistry
import org.openprojectx.spark.boot.autoconfigure.JdbcConnectionRegistry
import org.openprojectx.spark.boot.autoconfigure.S3Properties
import org.openprojectx.spark.boot.autoconfigure.SparkBootConfigLoader
import org.openprojectx.spark.boot.autoconfigure.SparkBootProperties
import org.openprojectx.spark.boot.connectors.JdbcSinkConfigFactory
import org.openprojectx.spark.boot.connectors.JdbcSinkNodeFactory
import org.openprojectx.spark.boot.connectors.JdbcSourceConfigFactory
import org.openprojectx.spark.boot.connectors.JdbcSourceNodeFactory
import org.openprojectx.spark.boot.connectors.IcebergSinkConfigFactory
import org.openprojectx.spark.boot.connectors.IcebergSinkNodeFactory
import org.openprojectx.spark.boot.connectors.ParquetSinkConfigFactory
import org.openprojectx.spark.boot.connectors.ParquetSinkNodeFactory
import org.openprojectx.spark.boot.connectors.ParquetSourceConfigFactory
import org.openprojectx.spark.boot.connectors.ParquetSourceNodeFactory
import org.openprojectx.spark.boot.connectors.SelectConfigFactory
import org.openprojectx.spark.boot.connectors.SelectNodeFactory
import org.openprojectx.spark.boot.connectors.SqlFilterConfigFactory
import org.openprojectx.spark.boot.connectors.SqlFilterNodeFactory
import org.openprojectx.spark.boot.connectors.SqlTransformConfigFactory
import org.openprojectx.spark.boot.connectors.SqlTransformNodeFactory
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.NodeFactoryRegistry
import org.openprojectx.spark.boot.core.ProfiledConfigNodeFactory
import org.openprojectx.spark.boot.core.ProfiledProgrammaticNodeFactory
import org.openprojectx.spark.boot.core.ProgrammaticNodeFactoryRegistry
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkRuntime

@Singleton
@Component(
    modules = [
        SparkModule::class,
        RuntimeModule::class,
        BuiltinConnectorModule::class
    ]
)
interface SparkBootComponent {
    fun sparkSession(): SparkSession
    fun sparkExecutionContext(): SparkExecutionContext
    fun sparkRuntime(): SparkRuntime
    fun nodeFactoryRegistry(): NodeFactoryRegistry
    fun programmaticNodeFactoryRegistry(): ProgrammaticNodeFactoryRegistry

    fun parquetSourceNodeFactory(): ParquetSourceNodeFactory
    fun parquetSinkNodeFactory(): ParquetSinkNodeFactory
    fun jdbcSourceNodeFactory(): JdbcSourceNodeFactory
    fun icebergSinkNodeFactory(): IcebergSinkNodeFactory
    fun sqlFilterNodeFactory(): SqlFilterNodeFactory
    fun selectNodeFactory(): SelectNodeFactory
    fun sqlTransformNodeFactory(): SqlTransformNodeFactory
    fun jdbcSinkNodeFactory(): JdbcSinkNodeFactory
}

@Module
object SparkModule {
    @Provides
    @Singleton
    fun provideSparkBootProperties(): SparkBootProperties {
        return SparkBootConfigLoader.load()
    }

    @Provides
    @Singleton
    fun provideJdbcConnectionRegistry(properties: SparkBootProperties): JdbcConnectionRegistry {
        return JdbcConnectionRegistry(properties.jdbcConnections)
    }

    @Provides
    @Singleton
    fun provideIcebergCatalogRegistry(properties: SparkBootProperties): IcebergCatalogRegistry {
        return IcebergCatalogRegistry(properties.icebergCatalogs)
    }

    @Provides
    @Singleton
    fun provideSparkSession(properties: SparkBootProperties): SparkSession {
        val builder = SparkSession.builder()
            .appName("spark-boot")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.planChangeValidation", "false")
            .config("spark.sql.lightweightPlanChangeValidation", "false")

        configureS3(builder, properties.s3)
        configureHiveMetastore(builder, properties.hms)
        configureIcebergCatalogs(builder, properties.icebergCatalogs.values)

        return builder.getOrCreate()
    }

    private fun configureS3(builder: SparkSession.Builder, s3: S3Properties?) {
        val endpoint = s3?.endpoint
            ?: System.getProperty("aws.endpoint-url.s3")
            ?: System.getenv("AWS_ENDPOINT_URL_S3")
            ?: return

        builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", (s3?.pathStyleAccess ?: true).toString())
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", (s3?.sslEnabled ?: false).toString())
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                s3?.credentialsProvider ?: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )

        val accessKey = s3?.accessKey
            ?: System.getProperty("aws.accessKeyId")
            ?: System.getenv("AWS_ACCESS_KEY_ID")
        val secretKey = s3?.secretKey
            ?: System.getProperty("aws.secretAccessKey")
            ?: System.getenv("AWS_SECRET_ACCESS_KEY")
        val region = s3?.region
            ?: System.getProperty("aws.region")
            ?: System.getenv("AWS_REGION")
            ?: "us-east-1"

        accessKey?.let { builder.config("spark.hadoop.fs.s3a.access.key", it) }
        secretKey?.let { builder.config("spark.hadoop.fs.s3a.secret.key", it) }
        builder.config("spark.hadoop.fs.s3a.endpoint.region", region)
    }

    private fun configureHiveMetastore(builder: SparkSession.Builder, hms: HmsProperties?) {
        val metastoreUris = hms?.uri
            ?: System.getProperty("hive.metastore.uris")
            ?: System.getenv("HIVE_METASTORE_URIS")
            ?: return
        val warehouse = hms?.warehouse
            ?: System.getProperty("spark.boot.iceberg.warehouse")
            ?: System.getenv("SPARK_BOOT_ICEBERG_WAREHOUSE")
            ?: "file:/tmp/spark-boot-iceberg-warehouse"
        val catalog = hms?.catalog ?: "hms"

        builder
            .enableHiveSupport()
            .config("hive.metastore.uris", metastoreUris)
            .config(
                "spark.driver.extraJavaOptions",
                "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED",
            )
            .config(
                "spark.executor.extraJavaOptions",
                "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED",
            )
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.$catalog.type", "hive")
            .config("spark.sql.catalog.$catalog.uri", metastoreUris)
            .config("spark.sql.catalog.$catalog.warehouse", warehouse)

        listOf(
            "hive.metastore.use.SSL",
            "hive.metastore.truststore.path",
            "hive.metastore.truststore.password",
            "hive.metastore.sasl.enabled",
            "hive.metastore.kerberos.principal"
        ).forEach { key ->
            System.getProperty(key)?.let { value ->
                builder.config(key, value)
                builder.config("spark.hadoop.$key", value)
            }
        }
    }

    private fun configureIcebergCatalogs(
        builder: SparkSession.Builder,
        catalogs: Collection<IcebergCatalogProperties>
    ) {
        if (catalogs.isEmpty()) {
            return
        }

        builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        catalogs.forEach { catalog ->
            val prefix = "spark.sql.catalog.${catalog.name}"
            builder.config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            builder.config("$prefix.type", catalog.type)
            catalog.uri?.let { builder.config("$prefix.uri", it) }
            catalog.warehouse?.let { builder.config("$prefix.warehouse", it) }
            catalog.properties.forEach { (key, value) ->
                builder.config("$prefix.$key", value)
            }
        }
    }

    @Provides
    @Singleton
    fun provideSparkExecutionContext(spark: SparkSession): SparkExecutionContext {
        return SparkExecutionContext(spark)
    }
}

@Module
abstract class RuntimeModule {
    @Multibinds
    abstract fun profiledConfigNodeFactories(): Set<ProfiledConfigNodeFactory>

    @Multibinds
    abstract fun profiledProgrammaticNodeFactories(): Set<ProfiledProgrammaticNodeFactory>

    companion object {
    @Provides
    @Singleton
    fun provideSparkRuntime(context: SparkExecutionContext): SparkRuntime {
        return SparkRuntime(context)
    }

    @Provides
    @Singleton
    fun provideNodeFactoryRegistry(
        factories: Map<String, @JvmSuppressWildcards ConfigNodeFactory>,
        profiledFactories: Set<@JvmSuppressWildcards ProfiledConfigNodeFactory>,
        properties: SparkBootProperties
    ): NodeFactoryRegistry {
        return NodeFactoryRegistry(factories, profiledFactories, properties.activeProfiles)
    }

    @Provides
    @Singleton
    fun provideProgrammaticNodeFactoryRegistry(
        factories: Map<String, @JvmSuppressWildcards UntypedNodeFactory>,
        profiledFactories: Set<@JvmSuppressWildcards ProfiledProgrammaticNodeFactory>,
        properties: SparkBootProperties
    ): ProgrammaticNodeFactoryRegistry {
        return ProgrammaticNodeFactoryRegistry(factories, profiledFactories, properties.activeProfiles)
    }
    }
}

@Module
interface BuiltinConnectorModule {
    @Binds
    @IntoMap
    @StringKey("ParquetSource")
    fun bindProgrammaticParquetSourceFactory(factory: ParquetSourceNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("ParquetSink")
    fun bindProgrammaticParquetSinkFactory(factory: ParquetSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("JdbcSource")
    fun bindProgrammaticJdbcSourceFactory(factory: JdbcSourceNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("IcebergSink")
    fun bindProgrammaticIcebergSinkFactory(factory: IcebergSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("SqlFilterTransform")
    fun bindProgrammaticSqlFilterFactory(factory: SqlFilterNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("SelectTransform")
    fun bindProgrammaticSelectFactory(factory: SelectNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("SqlTransform")
    fun bindProgrammaticSqlTransformFactory(factory: SqlTransformNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("JdbcSink")
    fun bindProgrammaticJdbcSinkFactory(factory: JdbcSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey("ParquetSource")
    fun bindParquetSourceFactory(factory: ParquetSourceConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("ParquetSink")
    fun bindParquetSinkFactory(factory: ParquetSinkConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("JdbcSource")
    fun bindJdbcSourceFactory(factory: JdbcSourceConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("IcebergSink")
    fun bindIcebergSinkFactory(factory: IcebergSinkConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("SqlFilterTransform")
    fun bindSqlFilterFactory(factory: SqlFilterConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("SelectTransform")
    fun bindSelectFactory(factory: SelectConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("SqlTransform")
    fun bindSqlTransformFactory(factory: SqlTransformConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey("JdbcSink")
    fun bindJdbcSinkFactory(factory: JdbcSinkConfigFactory): ConfigNodeFactory
}
