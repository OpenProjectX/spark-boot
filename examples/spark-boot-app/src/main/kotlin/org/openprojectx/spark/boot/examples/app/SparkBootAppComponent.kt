package org.openprojectx.spark.boot.examples.app

import dagger.Binds
import dagger.Component
import dagger.Module
import dagger.Provides
import dagger.multibindings.IntoMap
import dagger.multibindings.IntoSet
import dagger.multibindings.StringKey
import javax.inject.Singleton
import org.openprojectx.spark.boot.core.ProfiledProgrammaticNodeFactory
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

    companion object {
        @Provides
        @IntoSet
        fun provideLocalOrdersSourceFactory(
            factory: LocalOrdersSourceNodeFactory
        ): ProfiledProgrammaticNodeFactory {
            return ProfiledProgrammaticNodeFactory(
                kind = "ProfiledOrdersSource",
                profiles = setOf("local"),
                factory = factory
            )
        }

        @Provides
        @IntoSet
        fun provideCiOrdersSourceFactory(
            factory: CiOrdersSourceNodeFactory
        ): ProfiledProgrammaticNodeFactory {
            return ProfiledProgrammaticNodeFactory(
                kind = "ProfiledOrdersSource",
                profiles = setOf("ci"),
                factory = factory
            )
        }
    }
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
