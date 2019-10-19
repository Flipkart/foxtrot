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
import com.flipkart.foxtrot.core.alerts.AlertingSystemEventConsumer;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationManager;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationManager;
import com.flipkart.foxtrot.core.querystore.*;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.reroute.ClusterRerouteJob;
import com.flipkart.foxtrot.core.reroute.ClusterRerouteManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryManager;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.resources.*;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.oor.OorBundle;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
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

        final SwaggerBundleConfiguration swaggerBundleConfiguration = getSwaggerBundleConfiguration();

        bootstrap.addBundle(new SwaggerBundle<FoxtrotServerConfiguration>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(FoxtrotServerConfiguration configuration) {
                return swaggerBundleConfiguration;
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

        ExecutorService executorService = environment.lifecycle()
                .executorService("query-executor-%s")
                .minThreads(20)
                .maxThreads(30)
                .keepAliveTime(Duration.seconds(30))
                .build();
        ScheduledExecutorService scheduledExecutorService = environment.lifecycle()
                .scheduledExecutorService("cardinality-executor")
                .threads(1)
                .build();

        HbaseTableConnection hbaseTableConnection = new HbaseTableConnection(configuration.getHbase());
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(configuration.getElasticsearch());
        HazelcastConnection hazelcastConnection = new HazelcastConnection(configuration.getCluster());
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());
        CardinalityConfig cardinalityConfig = configuration.getCardinality();
        if(cardinalityConfig == null) {
            cardinalityConfig = new CardinalityConfig("false", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
        }
        EsIndexOptimizationConfig esIndexOptimizationConfig = configuration.getEsIndexOptimizationConfig();
        if(esIndexOptimizationConfig == null) {
            esIndexOptimizationConfig = new EsIndexOptimizationConfig();
        }
        ConsoleHistoryConfig consoleHistoryConfig = configuration.getConsoleHistoryConfig();
        if(consoleHistoryConfig == null) {
            consoleHistoryConfig = new ConsoleHistoryConfig();
        }
        CacheConfig cacheConfig = configuration.getCacheConfig();
        EmailConfig emailConfig = configuration.getEmailConfig();

        final ObjectMapper objectMapper = environment.getObjectMapper();
        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection,
                                                                                        objectMapper, cardinalityConfig
        );

        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(objectMapper, configuration.getTextNodeRemover()));
        DataStore dataStore = new HBaseDataStore(hbaseTableConnection, objectMapper, new DocumentTranslator(configuration.getHbase()));
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mutators, objectMapper,
                                                            cardinalityConfig
        );
        FqlStoreService fqlStoreService = new FqlStoreServiceImpl(elasticsearchConnection, objectMapper);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, objectMapper, cacheConfig));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection,
                                                              cacheManager, objectMapper, configuration.getElasticsearchTuningConfig());
        InternalEventBus eventBus = new GuavaInternalEventBus();
        eventBus.subscribe(new AlertingSystemEventConsumer(
                new EmailClient(emailConfig),
                new RichEmailBuilder(new StrSubstitutorEmailSubjectBuilder(),
                                     new StrSubstitutorEmailBodyBuilder())));

        ClusterRerouteManager clusterRerouteManager = new ClusterRerouteManager(
                elasticsearchConnection, configuration.getClusterRerouteConfig());

        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService,
                                                   ImmutableList.<ActionExecutionObserver>builder()
                                                           .add(new MetricRecorder())
                                                           .add(new ResponseCacheUpdater(cacheManager))
                                                           .add(new SlowQueryReporter())
                                                           .add(new EventPublisherActionExecutionObserver(eventBus))
                                                           .build());
        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getDeletionManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore, scheduledExecutorService,
                                                                          hazelcastConnection
        );
        CardinalityCalculationManager cardinalityCalculationManager = new CardinalityCalculationManager(tableMetadataManager,
                                                                                                        cardinalityConfig,
                                                                                                        hazelcastConnection,
                                                                                                        scheduledExecutorService
        );
        EsIndexOptimizationManager esIndexOptimizationManager = new EsIndexOptimizationManager(scheduledExecutorService,
                                                                                               esIndexOptimizationConfig,
                                                                                               elasticsearchConnection, hazelcastConnection
        );
        ConsoleHistoryManager consoleHistoryManager = new ConsoleHistoryManager(scheduledExecutorService, consoleHistoryConfig,
                                                                                elasticsearchConnection, hazelcastConnection, objectMapper);
        ClusterRerouteJob clusterRerouteJob = new ClusterRerouteJob(scheduledExecutorService, configuration.getClusterRerouteConfig(),
                                                                    clusterRerouteManager, hazelcastConnection);

        List<HealthCheck> healthChecks = new ArrayList<>();
        ClusterManager clusterManager = new ClusterManager(hazelcastConnection, healthChecks, configuration.getServerFactory());

        environment.lifecycle()
                .manage(hbaseTableConnection);
        environment.lifecycle()
                .manage(elasticsearchConnection);
        environment.lifecycle()
                .manage(hazelcastConnection);
        environment.lifecycle()
                .manage(tableMetadataManager);
        environment.lifecycle()
                .manage(analyticsLoader);
        environment.lifecycle()
                .manage(dataDeletionManager);
        environment.lifecycle()
                .manage(clusterManager);
        environment.lifecycle()
                .manage(cardinalityCalculationManager);
        environment.lifecycle()
                .manage(esIndexOptimizationManager);
        environment.lifecycle()
                .manage(consoleHistoryManager);
        environment.lifecycle()
                .manage(clusterRerouteJob);

        environment.jersey()
                .register(new DocumentResource(queryStore, configuration.getSegregationConfiguration()));
        environment.jersey()
                .register(new AsyncResource(cacheManager));
        environment.jersey()
                .register(new AnalyticsResource(executor));
        environment.jersey()
                .register(new TableManagerResource(tableManager));
        environment.jersey()
                .register(new TableFieldMappingResource(tableManager, tableMetadataManager));
        environment.jersey()
                .register(new ConsoleResource(new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        environment.jersey()
                .register(new ConsoleV2Resource(new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.jersey()
                .register(new FqlResource(fqlEngine, fqlStoreService));
        environment.jersey()
                .register(new ClusterInfoResource(clusterManager));
        environment.jersey()
                .register(new UtilResource(configuration));
        environment.jersey()
                .register(new ClusterHealthResource(queryStore, tableManager, tableMetadataManager));
        environment.jersey()
                .register(new CacheUpdateResource(executorService, tableMetadataManager));
        environment.jersey()
                .register(new ESClusterResource(clusterRerouteManager));
        environment.jersey()
                .register(new FlatResponseTextProvider());
        environment.jersey()
                .register(new FlatResponseCsvProvider());
        environment.jersey()
                .register(new FlatResponseErrorTextProvider());
        environment.jersey()
                .register(new FoxtrotExceptionMapper(objectMapper));

        // Enable CORS headers
        final FilterRegistration.Dynamic cors = environment.servlets()
                .addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        ((AbstractServerFactory)configuration.getServerFactory()).setJerseyRootPath("/foxtrot");

        MetricUtil.setup(environment.metrics());

    }

    private void configureObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.registerSubtypes(new NamedType(SimpleClusterDiscoveryConfig.class, "foxtrot_simple"));
        objectMapper.registerSubtypes(new NamedType(MarathonClusterDiscoveryConfig.class, "foxtrot_marathon"));
    }

    private SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        final SwaggerBundleConfiguration swaggerBundleConfiguration = new SwaggerBundleConfiguration();
        swaggerBundleConfiguration.setTitle("Foxtrot");
        swaggerBundleConfiguration.setResourcePackage("com.flipkart.foxtrot.server.resources");
        swaggerBundleConfiguration.setUriPrefix("/foxtrot");
        swaggerBundleConfiguration.setDescription("A store abstraction and analytics system for real-time event data.");
        return swaggerBundleConfiguration;
    }

}
