package com.flipkart.foxtrot.server.guice;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.healthcheck.ElasticSearchHealthCheck;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.metrics.core.HealthCheck;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public class FoxtrotModule extends AbstractModule {

    private final FoxtrotServerConfiguration configuration;
    private final Environment environment;

    public FoxtrotModule(FoxtrotServerConfiguration configuration, Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        // bind configurations
        bind(FoxtrotServerConfiguration.class).toInstance(configuration);
        bind(HbaseConfig.class).toInstance(configuration.getHbase());
        bind(ElasticsearchConfig.class).toInstance(configuration.getElasticsearch());
        bind(ClusterConfig.class).toInstance(configuration.getCluster());
        bind(DataDeletionManagerConfig.class).toInstance(configuration.getTableDataManagerConfig());
        bind(HttpConfiguration.class).toInstance(configuration.getHttpConfiguration());

        // Configure mapper
        configureObjectMapper(environment);
        bind(ObjectMapper.class).toInstance(environment.getObjectMapperFactory().build());

        // Configure executor service
        bind(ExecutorService.class).toInstance(environment.managedExecutorService("query-executor-%s", 20, 40, 30, TimeUnit.SECONDS));

        // bind Interfaces
        bind(TableMetadataManager.class).to(DistributedTableMetadataManager.class);
        bind(DataStore.class).to(HBaseDataStore.class);
        bind(QueryStore.class).to(ElasticsearchQueryStore.class);
        bind(TableManager.class).to(FoxtrotTableManager.class);
        bind(ConsolePersistence.class).to(ElasticsearchConsolePersistence.class);
        bind(CacheFactory.class).to(DistributedCacheFactory.class);
    }

    private void configureObjectMapper(Environment environment) {
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapperFactory().setSubtypeResolver(subtypeResolver);
    }

    @Provides
    @Inject
    public List<HealthCheck> healthChecks(ElasticsearchConnection elasticsearchConnection) {
        return Collections.singletonList(new ElasticSearchHealthCheck("ES Health Check", elasticsearchConnection));
    }
}
