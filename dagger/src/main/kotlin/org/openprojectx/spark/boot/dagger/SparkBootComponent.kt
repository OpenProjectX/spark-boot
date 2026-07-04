package org.openprojectx.spark.boot.dagger

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.Provides
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import javax.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.openprojectx.spark.boot.connectors.JdbcSinkConfigFactory
import org.openprojectx.spark.boot.connectors.JdbcSinkNodeFactory
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

    fun parquetSourceNodeFactory(): ParquetSourceNodeFactory
    fun parquetSinkNodeFactory(): ParquetSinkNodeFactory
    fun sqlFilterNodeFactory(): SqlFilterNodeFactory
    fun selectNodeFactory(): SelectNodeFactory
    fun sqlTransformNodeFactory(): SqlTransformNodeFactory
    fun jdbcSinkNodeFactory(): JdbcSinkNodeFactory
}

@Module
object SparkModule {
    @Provides
    @Singleton
    fun provideSparkSession(): SparkSession {
        return SparkSession.builder()
            .appName("spark-boot")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
    }

    @Provides
    @Singleton
    fun provideSparkExecutionContext(spark: SparkSession): SparkExecutionContext {
        return SparkExecutionContext(spark)
    }
}

@Module
object RuntimeModule {
    @Provides
    @Singleton
    fun provideSparkRuntime(context: SparkExecutionContext): SparkRuntime {
        return SparkRuntime(context)
    }

    @Provides
    @Singleton
    fun provideNodeFactoryRegistry(
        factories: Map<String, @JvmSuppressWildcards ConfigNodeFactory>
    ): NodeFactoryRegistry {
        return NodeFactoryRegistry(factories)
    }
}

@Module
interface BuiltinConnectorModule {
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
