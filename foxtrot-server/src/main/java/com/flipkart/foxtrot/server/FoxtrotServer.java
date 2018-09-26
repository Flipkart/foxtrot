/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationManager;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationManager;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.resources.*;
import com.flipkart.foxtrot.sql.FqlEngine;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.discovery.bundle.ServiceDiscoveryBundle;
import io.dropwizard.discovery.bundle.ServiceDiscoveryConfiguration;
import io.dropwizard.oor.OorBundle;
import io.dropwizard.riemann.RiemannBundle;
import io.dropwizard.riemann.RiemannConfig;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;


/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {

    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
        bootstrap.addBundle(new AssetsBundle("/console/", "/", "index.html", "console"));
        bootstrap.addBundle(new OorBundle<FoxtrotServerConfiguration>() {
            public boolean withOor() {
                return false;
            }
        });

        bootstrap.addBundle(new ServiceDiscoveryBundle<FoxtrotServerConfiguration>() {
            @Override
            protected ServiceDiscoveryConfiguration getRangerConfiguration(FoxtrotServerConfiguration configuration) {
                return configuration.getServiceDiscovery();
            }

            @Override
            protected String getServiceName(FoxtrotServerConfiguration configuration) {
                return "foxtrot";
            }

            @Override
            protected int getPort(FoxtrotServerConfiguration configuration) {
                return configuration.getServiceDiscovery().getPublishedPort();
            }
        });

        bootstrap.addBundle(new RiemannBundle<FoxtrotServerConfiguration>() {
            @Override
            public RiemannConfig getRiemannConfiguration(FoxtrotServerConfiguration configuration) {
                return configuration.getRiemann();
            }
        });

        bootstrap.addCommand(new InitializerCommand());
        configureObjectMapper(bootstrap.getObjectMapper());
    }

    @Override
    public String getName() {
        return "foxtrot";
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {

        ExecutorService executorService = environment.lifecycle().executorService("query-executor-%s")
                .minThreads(20).maxThreads(30).keepAliveTime(Duration.seconds(30))
                .build();
        ScheduledExecutorService scheduledExecutorService =
                environment.lifecycle().scheduledExecutorService("cardinality-executor").threads(1).build();

        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(configuration.getHbase());
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(configuration.getElasticsearch());
        HazelcastConnection hazelcastConnection = new HazelcastConnection(configuration.getCluster());
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());
        CardinalityConfig cardinalityConfig = configuration.getCardinality();
        if (cardinalityConfig == null) {
            cardinalityConfig = new CardinalityConfig("false", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
        }
        EsIndexOptimizationConfig esIndexOptimizationConfig = configuration.getEsIndexOptimizationConfig();
        if(esIndexOptimizationConfig == null) {
            esIndexOptimizationConfig = new EsIndexOptimizationConfig();
        }
        CacheConfig cacheConfig = configuration.getCacheConfig();

        final ObjectMapper objectMapper = environment.getObjectMapper();
        TableMetadataManager tableMetadataManager =
                new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, objectMapper, cardinalityConfig);
        DataStore dataStore = new HBaseDataStore(HBaseTableConnection,
                                                 objectMapper, new DocumentTranslator(configuration.getHbase()));
        QueryStore queryStore =
                new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, objectMapper, cardinalityConfig);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, objectMapper, cacheConfig));
        AnalyticsLoader analyticsLoader =
                new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, objectMapper);
        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);
        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getTableDataManagerConfig();
        DataDeletionManager dataDeletionManager =
                new DataDeletionManager(dataDeletionManagerConfig, queryStore, scheduledExecutorService, hazelcastConnection);
        CardinalityCalculationManager cardinalityCalculationManager =
                new CardinalityCalculationManager(tableMetadataManager, cardinalityConfig, hazelcastConnection, scheduledExecutorService);
        EsIndexOptimizationManager esIndexOptimizationManager =
                new EsIndexOptimizationManager(scheduledExecutorService, esIndexOptimizationConfig, elasticsearchConnection,
                                               hazelcastConnection);

        List<HealthCheck> healthChecks = new ArrayList<>();
        //        ElasticSearchHealthCheck elasticSearchHealthCheck = new ElasticSearchHealthCheck(elasticsearchConnection);
        //        healthChecks.add(elasticSearchHealthCheck);
        ClusterManager clusterManager = new ClusterManager(hazelcastConnection, healthChecks, configuration.getServerFactory());

        environment.lifecycle().manage(HBaseTableConnection);
        environment.lifecycle().manage(elasticsearchConnection);
        environment.lifecycle().manage(hazelcastConnection);
        environment.lifecycle().manage(tableMetadataManager);
        environment.lifecycle().manage(analyticsLoader);
        environment.lifecycle().manage(dataDeletionManager);
        environment.lifecycle().manage(clusterManager);
        environment.lifecycle().manage(cardinalityCalculationManager);
        environment.lifecycle().manage(esIndexOptimizationManager);

        environment.jersey().register(new DocumentResource(queryStore));
        environment.jersey().register(new AsyncResource(cacheManager));
        environment.jersey().register(new AnalyticsResource(executor));
        environment.jersey().register(new TableManagerResource(tableManager));
        environment.jersey().register(new TableFieldMappingResource(tableManager, tableMetadataManager));
        environment.jersey().register(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        environment.jersey().register(new ConsoleV2Resource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.jersey().register(new FqlResource(fqlEngine));
        environment.jersey().register(new ClusterInfoResource(clusterManager));
        environment.jersey().register(new UtilResource(configuration));
        environment.jersey().register(new ClusterHealthResource(queryStore));
        environment.jersey().register(new CacheUpdateResource(executorService, tableMetadataManager));

        //        environment.healthChecks().register("ES Health Check", elasticSearchHealthCheck);

        environment.jersey().register(new FlatResponseTextProvider());
        environment.jersey().register(new FlatResponseCsvProvider());
        environment.jersey().register(new FlatResponseErrorTextProvider());
        environment.jersey().register(new FoxtrotExceptionMapper(objectMapper));

        // Enable CORS headers
        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        ((AbstractServerFactory) configuration.getServerFactory()).setJerseyRootPath("/foxtrot");

        MetricUtil.setup(environment.metrics());
    }

    private void configureObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.registerSubtypes(new NamedType(SimpleClusterDiscoveryConfig.class, "foxtrot_simple"));
        objectMapper.registerSubtypes(new NamedType(MarathonClusterDiscoveryConfig.class, "foxtrot_marathon"));
    }

}
