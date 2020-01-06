package com.flipkart.foxtrot.server.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.config.SegregationConfiguration;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.dropwizard.setup.Environment;

public class FoxtrotConfigurationModule extends AbstractModule {

    @Override
    protected void configure() {

    }

    @Provides
    @Singleton
    HbaseConfig provideHbaseConfig(FoxtrotServerConfiguration foxtrotServerConfiguration) {
        return foxtrotServerConfiguration.getHbase();
    }

    @Provides
    @Singleton
    ElasticsearchConfig provideElasticsearchConfig(FoxtrotServerConfiguration foxtrotServerConfiguration) {
        return foxtrotServerConfiguration.getElasticsearch();
    }

    @Provides
    @Singleton
    CardinalityConfig provideCardinalityConfig(FoxtrotServerConfiguration foxtrotServerConfiguration) {
        return foxtrotServerConfiguration.getCardinality() != null
                ? foxtrotServerConfiguration.getCardinality()
                : new CardinalityConfig("false", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
    }

    @Provides
    @Singleton
    ClusterConfig provideClusterConfig(FoxtrotServerConfiguration foxtrotServerConfiguration) {
        return foxtrotServerConfiguration.getCluster();
    }

    @Provides
    @Singleton
    TextNodeRemoverConfiguration provideTextNodeRemoverConfiguration(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getTextNodeRemover();
    }

    @Provides
    @Singleton
    EsIndexOptimizationConfig provideEsIndexOptimizationConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getEsIndexOptimizationConfig() != null
                ? serverConfiguration.getEsIndexOptimizationConfig()
                : new EsIndexOptimizationConfig();
    }

    @Provides
    @Singleton
    ConsoleHistoryConfig provideConsoleHistoryConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getConsoleHistoryConfig() != null
                ? serverConfiguration.getConsoleHistoryConfig()
                : new ConsoleHistoryConfig();
    }

    @Provides
    @Singleton
    CacheConfig provideCacheConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getCacheConfig();
    }

    @Provides
    @Singleton
    EmailConfig provideEmailConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getEmailConfig();
    }

    @Provides
    @Singleton
    public ObjectMapper provideObjectMapper(Environment environment) {
        return environment.getObjectMapper();
    }

    @Provides
    @Singleton
    DataDeletionManagerConfig provideDataDeletionManagerConfig(FoxtrotServerConfiguration foxtrotServerConfiguration) {
        return foxtrotServerConfiguration.getDeletionManagerConfig();
    }

    @Provides
    @Singleton
    SegregationConfiguration provideSegregationConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getSegregationConfiguration();
    }
}
