package org.openprojectx.sparkboot.dagger

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.Provides
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import javax.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.openprojectx.sparkboot.connectors.JdbcSinkConfigFactory
import org.openprojectx.sparkboot.connectors.JdbcSinkNodeFactory
import org.openprojectx.sparkboot.connectors.ParquetSinkConfigFactory
import org.openprojectx.sparkboot.connectors.ParquetSinkNodeFactory
import org.openprojectx.sparkboot.connectors.ParquetSourceConfigFactory
import org.openprojectx.sparkboot.connectors.ParquetSourceNodeFactory
import org.openprojectx.sparkboot.connectors.SelectConfigFactory
import org.openprojectx.sparkboot.connectors.SelectNodeFactory
import org.openprojectx.sparkboot.connectors.SqlFilterConfigFactory
import org.openprojectx.sparkboot.connectors.SqlFilterNodeFactory
import org.openprojectx.sparkboot.connectors.SqlTransformConfigFactory
import org.openprojectx.sparkboot.connectors.SqlTransformNodeFactory
import org.openprojectx.sparkboot.core.ConfigNodeFactory
import org.openprojectx.sparkboot.core.NodeFactoryRegistry
import org.openprojectx.sparkboot.runtime.spark.SparkExecutionContext
import org.openprojectx.sparkboot.runtime.spark.SparkRuntime

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
