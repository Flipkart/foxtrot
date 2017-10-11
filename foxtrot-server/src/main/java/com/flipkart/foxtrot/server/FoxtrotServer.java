/**
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
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.healthcheck.ElasticSearchHealthCheck;
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


/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {

    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)
                )
        );
        bootstrap.addBundle(new AssetsBundle("/console/", "/", "index.html", "console"));
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
        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(configuration.getHbase());
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(configuration.getElasticsearch());
        HazelcastConnection hazelcastConnection = new HazelcastConnection(configuration.getCluster());
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        DataStore dataStore = new HBaseDataStore(HBaseTableConnection,
                environment.getObjectMapper(), new DocumentTranslator(configuration.getHbase()));
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, environment.getObjectMapper());
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, environment.getObjectMapper()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, environment.getObjectMapper());
        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);
        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getTableDataManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore);

        List<HealthCheck> healthChecks = new ArrayList<>();
        ElasticSearchHealthCheck elasticSearchHealthCheck = new ElasticSearchHealthCheck(elasticsearchConnection);
        healthChecks.add(elasticSearchHealthCheck);
        ClusterManager clusterManager = new ClusterManager(hazelcastConnection, healthChecks, configuration.getServerFactory());

        environment.lifecycle().manage(HBaseTableConnection);
        environment.lifecycle().manage(elasticsearchConnection);
        environment.lifecycle().manage(hazelcastConnection);
        environment.lifecycle().manage(tableMetadataManager);
        environment.lifecycle().manage(analyticsLoader);
        environment.lifecycle().manage(dataDeletionManager);
        environment.lifecycle().manage(clusterManager);

        environment.jersey().register(new DocumentResource(queryStore));
        environment.jersey().register(new AsyncResource(cacheManager));
        environment.jersey().register(new AnalyticsResource(executor));
        environment.jersey().register(new TableManagerResource(tableManager));
        environment.jersey().register(new TableFieldMappingResource(tableManager, queryStore));
        environment.jersey().register(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, environment.getObjectMapper())));
        environment.jersey().register(new ConsoleV2Resource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, environment.getObjectMapper())));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, environment.getObjectMapper());
        environment.jersey().register(new FqlResource(fqlEngine));
        environment.jersey().register(new ClusterInfoResource(clusterManager));
        environment.jersey().register(new UtilResource(configuration));
        environment.jersey().register(new ClusterHealthResource(queryStore));

        environment.healthChecks().register("ES Health Check", elasticSearchHealthCheck);

        environment.jersey().register(new FlatResponseTextProvider());
        environment.jersey().register(new FlatResponseCsvProvider());
        environment.jersey().register(new FlatResponseErrorTextProvider());
        environment.jersey().register(new FoxtrotExceptionMapper(environment.getObjectMapper()));

        // Enable CORS headers
        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        ((AbstractServerFactory)configuration.getServerFactory()).setJerseyRootPath("/foxtrot");
    }

    private void configureObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.registerSubtypes(new NamedType(SimpleClusterDiscoveryConfig.class, "foxtrot_simple"));
        objectMapper.registerSubtypes(new NamedType(MarathonClusterDiscoveryConfig.class, "foxtrot_marathon"));
    }

}
