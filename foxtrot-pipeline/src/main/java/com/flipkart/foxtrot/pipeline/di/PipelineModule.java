package com.flipkart.foxtrot.pipeline.di;

import com.flipkart.foxtrot.pipeline.PipelineConfiguration;
import com.flipkart.foxtrot.pipeline.PipelineConstants;
import com.flipkart.foxtrot.pipeline.geo.ESRIGeoRegionIndex;
import com.flipkart.foxtrot.pipeline.geo.GeoRegionIndex;
import com.flipkart.foxtrot.pipeline.processors.factory.CachedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.factory.ProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReader;
import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReaderImpl;
import com.flipkart.foxtrot.pipeline.resources.GeojsonStore;
import com.flipkart.foxtrot.pipeline.resources.GeojsonStoreImpl;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import java.util.EnumSet;
import java.util.Set;

public class PipelineModule extends AbstractModule {

    public PipelineModule() {
        configureJsonPath();
    }

    @Override
    protected void configure() {
        bind(GeoJsonReader.class).to(GeoJsonReaderImpl.class)
                .in(Singleton.class);
        bind(ProcessorFactory.class).to(CachedProcessorFactory.class)
                .in(Singleton.class);
        bind(ProcessorFactory.class).annotatedWith(Names.named(PipelineConstants.REFLECTION_BASED_PROCESSOR_FACTORY))
                .to(ReflectionBasedProcessorFactory.class)
                .in(Singleton.class);
        bind(GeoRegionIndex.class).to(ESRIGeoRegionIndex.class);
        bind(GeojsonStore.class).to(GeojsonStoreImpl.class)
                .in(Singleton.class);
    }

    private void configureJsonPath() {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });
    }

    @Provides
    @Named(PipelineConstants.PROCESSOR_CACHE_BUILDER)
    private CacheBuilder providesHandlerCacheBuilder(Provider<PipelineConfiguration> configurationProvider) {
        String cacheSpec = Strings.isNullOrEmpty(configurationProvider.get()
                .getHandlerCacheSpec())
                ? PipelineConstants.HandlerCache.CACHE_SPEC
                : configurationProvider.get()
                .getHandlerCacheSpec();
        return CacheBuilder.from(cacheSpec);
    }


}
