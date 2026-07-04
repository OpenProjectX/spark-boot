package org.openprojectx.spark.boot.examples.app

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import javax.inject.Singleton
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.boot.dagger.BuiltinConnectorModule
import org.openprojectx.spark.boot.dagger.RuntimeModule
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dagger.SparkModule

@Module
interface SparkBootAppModule {
    @Binds
    @IntoMap
    @StringKey("InMemoryOrdersSource")
    fun bindInMemoryOrdersSourceFactory(factory: InMemoryOrdersSourceNodeFactory): UntypedNodeFactory
}

@Singleton
@Component(
    modules = [
        SparkModule::class,
        RuntimeModule::class,
        BuiltinConnectorModule::class,
        SparkBootAppModule::class
    ]
)
interface SparkBootAppComponent : SparkBootComponent
