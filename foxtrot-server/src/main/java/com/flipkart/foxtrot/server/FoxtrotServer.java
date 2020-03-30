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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
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
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.MarathonClusterDiscoveryConfig;
import com.flipkart.foxtrot.core.querystore.impl.SimpleClusterDiscoveryConfig;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.flipkart.foxtrot.gandalf.access.AccessService;
import com.flipkart.foxtrot.gandalf.access.AccessServiceImpl;
import com.flipkart.foxtrot.gandalf.manager.GandalfManager;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.di.FoxtrotModule;
import com.google.inject.Stage;
import com.flipkart.foxtrot.server.config.GandalfConfiguration;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.phonepe.gandalf.client.GandalfBundle;
import com.phonepe.gandalf.client.GandalfClient;
import com.phonepe.gandalf.models.client.GandalfClientConfig;
import com.phonepe.platform.http.OkHttpUtils;
import com.phonepe.platform.http.ServiceEndpointProvider;
import com.phonepe.platform.http.ServiceEndpointProviderFactory;
import com.phonepe.rosey.dwconfig.RoseyConfigSourceProvider;
import io.appform.dropwizard.discovery.bundle.ServiceDiscoveryBundle;
import io.appform.dropwizard.discovery.bundle.ServiceDiscoveryConfiguration;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.oor.OorBundle;
import io.dropwizard.primer.model.PrimerBundleConfiguration;
import io.dropwizard.riemann.RiemannBundle;
import io.dropwizard.riemann.RiemannConfig;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import ru.vyarus.dropwizard.guice.GuiceBundle;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {

    private ServiceDiscoveryBundle<FoxtrotServerConfiguration> serviceDiscoveryBundle;

    @Override
    public String getName() {
        return "foxtrot";
    }

    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        boolean localConfig = Boolean.parseBoolean(System.getProperty("localConfig", "false"));
        if (localConfig) {
            bootstrap.setConfigurationSourceProvider(
                    new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                                                   new EnvironmentVariableSubstitutor()));
        }
        else {
            bootstrap.setConfigurationSourceProvider(
                    new SubstitutingSourceProvider(new RoseyConfigSourceProvider("platform", "foxtrot"),
                                                   new EnvironmentVariableSubstitutor()));
        }
        bootstrap.addBundle(new AssetsBundle("/console/", "/", "index.html", "console"));
        bootstrap.addBundle(new OorBundle<FoxtrotServerConfiguration>() {
            public boolean withOor() {
                return false;
            }
        });

        this.serviceDiscoveryBundle = new ServiceDiscoveryBundle<FoxtrotServerConfiguration>() {
            @Override
            protected ServiceDiscoveryConfiguration getRangerConfiguration(FoxtrotServerConfiguration configuration) {
                return configuration.getServiceDiscovery();
            }

            @Override
            protected String getServiceName(FoxtrotServerConfiguration configuration) {
                if (configuration.getRangerConfiguration() != null && configuration.getRangerConfiguration()
                        .getServiceName() != null) {
                    return configuration.getRangerConfiguration()
                            .getServiceName();
                }
                return "foxtrot-es6";
            }

            @Override
            protected int getPort(FoxtrotServerConfiguration configuration) {
                return configuration.getServiceDiscovery()
                        .getPublishedPort();
            }
        };
        bootstrap.addBundle(serviceDiscoveryBundle);

        GandalfBundle gandalfBundle = new GandalfBundle<FoxtrotServerConfiguration>() {
            @Override
            protected CuratorFramework getCuratorFramework() {
                return serviceDiscoveryBundle.getCurator();
            }

            @Override
            protected GandalfClientConfig getGandalfClientConfig(
                    FoxtrotServerConfiguration foxtrotServerConfiguration) {
                return foxtrotServerConfiguration.getGandalfConfig();
            }

            @Override
            protected PrimerBundleConfiguration getGandalfPrimerConfig(
                    FoxtrotServerConfiguration foxtrotServerConfiguration) {
                return foxtrotServerConfiguration.getPrimerBundleConfiguration();
            }
        };

        bootstrap.addBundle(gandalfBundle);

        bootstrap.addBundle(new RiemannBundle<FoxtrotServerConfiguration>() {
            @Override
            public RiemannConfig getRiemannConfiguration(FoxtrotServerConfiguration configuration) {
                return configuration.getRiemann();
            }
        });

        bootstrap.addBundle(new SwaggerBundle<FoxtrotServerConfiguration>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(FoxtrotServerConfiguration configuration) {
                return configuration.getSwagger();
            }
        });

        bootstrap.addCommand(new InitializerCommand());
        configureObjectMapper(bootstrap.getObjectMapper());
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
        if (cardinalityConfig == null) {
            cardinalityConfig = new CardinalityConfig("false",
                                                      String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
        }
        EsIndexOptimizationConfig esIndexOptimizationConfig = configuration.getEsIndexOptimizationConfig();
        if (esIndexOptimizationConfig == null) {
            esIndexOptimizationConfig = new EsIndexOptimizationConfig();
        }
        ConsoleHistoryConfig consoleHistoryConfig = configuration.getConsoleHistoryConfig();
        if (consoleHistoryConfig == null) {
            consoleHistoryConfig = new ConsoleHistoryConfig();
        }
        CacheConfig cacheConfig = configuration.getCacheConfig();
        EmailConfig emailConfig = configuration.getEmailConfig();

        final ObjectMapper objectMapper = environment.getObjectMapper();
        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                                                                                        elasticsearchConnection,
                                                                                        objectMapper,
                                                                                        cardinalityConfig);
        DataStore dataStore = new HBaseDataStore(hbaseTableConnection, objectMapper,
                                                 new DocumentTranslator(configuration.getHbase()));

        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(objectMapper,
                                                                                         configuration.getTextNodeRemover()));
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore,
                                                            mutators, objectMapper, cardinalityConfig);
        TableActionRequestVisitor actionRequestVisitor = new TableActionRequestVisitor();
        AccessService accessService = new AccessServiceImpl(configuration.isRestrictAccess(), actionRequestVisitor);
        FqlStoreService fqlStoreService = new FqlStoreServiceImpl(elasticsearchConnection, objectMapper);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection,
                                                                                 objectMapper,
                                                                                 cacheConfig));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager,
                                                              dataStore,
                                                              queryStore,
                                                              elasticsearchConnection,
                                                              cacheManager,
                                                              objectMapper,
                                                              configuration.getElasticsearchTuningConfig());
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
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore,
                                                                          scheduledExecutorService,
                                                                          hazelcastConnection);
        CardinalityCalculationManager cardinalityCalculationManager = new CardinalityCalculationManager(
                tableMetadataManager, cardinalityConfig, hazelcastConnection, scheduledExecutorService);
        EsIndexOptimizationManager esIndexOptimizationManager = new EsIndexOptimizationManager(scheduledExecutorService,
                                                                                               esIndexOptimizationConfig,
                                                                                               elasticsearchConnection,
                                                                                               hazelcastConnection);
        ConsoleHistoryManager consoleHistoryManager = new ConsoleHistoryManager(scheduledExecutorService,
                                                                                consoleHistoryConfig,
                                                                                elasticsearchConnection,
                                                                                hazelcastConnection, objectMapper);
        ClusterRerouteJob clusterRerouteJob = new ClusterRerouteJob(scheduledExecutorService,
                                                                    configuration.getClusterRerouteConfig(),
                                                                    clusterRerouteManager,
                                                                    hazelcastConnection);

        List<HealthCheck> healthChecks = new ArrayList<>();
        ClusterManager clusterManager = new ClusterManager(hazelcastConnection, healthChecks,
                                                           configuration.getServerFactory());

        ServiceEndpointProviderFactory serviceEndpointFactory = new ServiceEndpointProviderFactory(this.serviceDiscoveryBundle
                                                                                                           .getCurator());
        ServiceEndpointProvider gandalfEndpoint = serviceEndpointFactory.provider(configuration.getGandalfConfig()
                                                                                          .getHttpConfig(),
                                                                                  environment);

        OkHttpClient okHttp = OkHttpUtils.createDefaultClient("foxtrot-gandalf-client",
                                                              environment.metrics(),
                                                              configuration.getGandalfConfig().getHttpConfig());
        GandalfManager gandalfManager = new GandalfManager(environment.getObjectMapper(),
                                                           okHttp,
                                                           gandalfEndpoint,
                                                           configuration.getGandalfConfiguration().getUsername(),
                                                           configuration.getGandalfConfiguration().getPassword());

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
                .register(new AnalyticsResource(executor, configuration.getQueryConfig()));
        environment.jersey()
                .register(new AnalyticsV2Resource(executor, accessService, configuration.getQueryConfig()));
        environment.jersey()
                .register(new TableManagerResource(tableManager));
        environment.jersey()
                .register(new TableManagerV2Resource(tableManager, gandalfManager));
        environment.jersey()
                .register(new TableFieldMappingResource(tableManager, tableMetadataManager));
        environment.jersey()
                .register(new ConsoleResource(
                        new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        environment.jersey()
                .register(new ConsoleV2Resource(
                        new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.jersey()
                .register(new FqlResource(fqlEngine, fqlStoreService, accessService, configuration.getQueryConfig()));
        environment.jersey()
                .register(new FqlV2Resource(fqlEngine, fqlStoreService, accessService, configuration.getQueryConfig()));
        environment.jersey()
                .register(new HbaseRegionsMergeResource(configuration.getHbase()));
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

        ((AbstractServerFactory) configuration.getServerFactory()).setJerseyRootPath("/foxtrot");

        MetricUtil.setup(environment.metrics());
        GandalfConfiguration gandalfConfiguration = configuration.getGandalfConfiguration();
        if (gandalfConfiguration != null && StringUtils.isNotEmpty(gandalfConfiguration.getRedirectUrl())) {
            GandalfClient.initializeUrlPatternsAuthentication(gandalfConfiguration.getRedirectUrl(),
                                                              gandalfConfiguration.getServiceBaseUrl(),
                                                              "/echo/*",
                                                              "/cluster/*",
                                                              "/fql/*",
                                                              "/",
                                                              "/index.html");
        }
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());

    }

    private void configureObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.registerSubtypes(new NamedType(SimpleClusterDiscoveryConfig.class, "foxtrot_simple"));
        objectMapper.registerSubtypes(new NamedType(MarathonClusterDiscoveryConfig.class, "foxtrot_marathon"));
    }

}
