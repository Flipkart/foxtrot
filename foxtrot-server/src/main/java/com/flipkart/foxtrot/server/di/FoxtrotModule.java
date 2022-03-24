package com.flipkart.foxtrot.server.di;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.core.alerts.AlertingSystemEventConsumer;
import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.*;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.config.*;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.EmailSubjectBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.persistence.ElasticsearchFunnelStore;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.funnel.services.FunnelService;
import com.flipkart.foxtrot.core.funnel.services.FunnelServiceImplV1;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.impl.IndexMetadataManagerImpl;
import com.flipkart.foxtrot.core.indexmeta.impl.TableIndexMetadataServiceImpl;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.lock.HazelcastDistributedLockConfig;
import com.flipkart.foxtrot.core.nodegroup.AllocationManager;
import com.flipkart.foxtrot.core.nodegroup.AllocationManagerImpl;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManagerImpl;
import com.flipkart.foxtrot.core.nodegroup.repository.NodeGroupRepository;
import com.flipkart.foxtrot.core.nodegroup.repository.NodeGroupRepositoryImpl;
import com.flipkart.foxtrot.core.pipeline.PipelineManager;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManagerPipelineFetcherAdaptor;
import com.flipkart.foxtrot.core.pipeline.impl.DistributedPipelineMetadataManager;
import com.flipkart.foxtrot.core.pipeline.impl.FoxtrotPipelineManager;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.shardtuning.ShardCountTuningService;
import com.flipkart.foxtrot.core.shardtuning.ShardCountTuningServiceImpl;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.core.tenant.TenantManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.flipkart.foxtrot.core.tenant.impl.DistributedTenantMetadataManager;
import com.flipkart.foxtrot.core.tenant.impl.FoxtrotTenantManager;
import com.flipkart.foxtrot.pipeline.resolver.PipelineFetcher;
import com.flipkart.foxtrot.pipeline.resources.GeojsonStoreConfiguration;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.auth.ESAuthStore;
import com.flipkart.foxtrot.server.auth.IdmanAuthStore;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.NoneAuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.impl.IdmanAuthProvider;
import com.flipkart.foxtrot.server.auth.sessionstore.DistributedSessionDataStore;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.console.QueryManager;
import com.flipkart.foxtrot.server.console.QueryManagerImpl;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public class FoxtrotModule extends AbstractModule {

    @Override
    protected void configure() {
//        bind(AuthConfig.class);
        bind(ESAuthStore.class);
        bind(IdmanAuthStore.class);
        bind(GoogleAuthProvider.class);
        bind(IdmanAuthProvider.class);
        bind(NoneAuthProvider.class);
        bind(AuthProvider.class).to(GoogleAuthProvider.class);
        bind(AuthStore.class).to(ESAuthStore.class);
        bind(SessionDataStore.class).to(DistributedSessionDataStore.class);
        bind(TableMetadataManager.class).to(DistributedTableMetadataManager.class);
        bind(TenantMetadataManager.class).to(DistributedTenantMetadataManager.class);
        bind(PipelineMetadataManager.class).to(DistributedPipelineMetadataManager.class);
        bind(DataStore.class).to(HBaseDataStore.class);
        bind(QueryStore.class).to(ElasticsearchQueryStore.class);
        bind(FqlStoreService.class).to(FqlStoreServiceImpl.class);
        bind(CacheFactory.class).to(DistributedCacheFactory.class);
        bind(InternalEventBusConsumer.class).to(AlertingSystemEventConsumer.class);
        bind(ConsolePersistence.class).to(ElasticsearchConsolePersistence.class);
        bind(TableManager.class).to(FoxtrotTableManager.class);
        bind(TenantManager.class).to(FoxtrotTenantManager.class);
        bind(PipelineManager.class).to(FoxtrotPipelineManager.class);
        bind(QueryManager.class).to(QueryManagerImpl.class);

        bind(new TypeLiteral<ActionRequestVisitor<String>>() {
        }).toInstance(new TableActionRequestVisitor());
        bind(FunnelService.class).annotatedWith(Names.named("FunnelServiceImplV1"))
                .to(FunnelServiceImplV1.class);
        bind(FunnelService.class).to(FunnelServiceImplV1.class);
        bind(FunnelStore.class).to(ElasticsearchFunnelStore.class);
        bind(new TypeLiteral<List<HealthCheck>>() {
        }).toProvider(HealthcheckListProvider.class);
        bind(CardinalityValidator.class).to(CardinalityValidatorImpl.class);
        bind(CardinalityCalculationService.class).to(CardinalityCalculationServiceImpl.class);
        bind(NodeGroupManager.class).to(NodeGroupManagerImpl.class);
        bind(AllocationManager.class).to(AllocationManagerImpl.class);
        bind(NodeGroupRepository.class).to(NodeGroupRepositoryImpl.class);
        bind(PipelineFetcher.class).to(PipelineMetadataManagerPipelineFetcherAdaptor.class);
        bind(IndexMetadataManager.class).to(IndexMetadataManagerImpl.class);
        bind(TableIndexMetadataService.class).to(TableIndexMetadataServiceImpl.class);
        bind(ShardCountTuningService.class).to(ShardCountTuningServiceImpl.class);
        bind(PipelineFetcher.class).to(PipelineMetadataManagerPipelineFetcherAdaptor.class);
    }

    @Provides
    @Singleton
    public HbaseConfig hbConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getHbase();
    }

    @Provides
    @Singleton
    public ElasticsearchConfig esConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getElasticsearch();
    }

    @Provides
    @Singleton
    public ClusterConfig clusterConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getCluster();
    }

    @Provides
    @Singleton
    public CardinalityConfig cardinalityConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getCardinality()
                ? new CardinalityConfig("false", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE))
                : configuration.getCardinality();
    }

    @Provides
    @Singleton
    public EsIndexOptimizationConfig esIndexOptimizationConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getEsIndexOptimizationConfig()
                ? new EsIndexOptimizationConfig()
                : configuration.getEsIndexOptimizationConfig();
    }

    @Provides
    @Singleton
    public DataDeletionManagerConfig dataDeletionManagerConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getDeletionManagerConfig();
    }

    @Provides
    @Singleton
    public ConsoleHistoryConfig consoleHistoryConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getConsoleHistoryConfig()
                ? new ConsoleHistoryConfig()
                : configuration.getConsoleHistoryConfig();
    }

    @Provides
    @Singleton
    public ShardRebalanceJobConfig shardRebalanceJobConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getShardRebalanceJobConfig()
                ? new ShardRebalanceJobConfig()
                : configuration.getShardRebalanceJobConfig();
    }

    @Provides
    @Singleton
    public CacheConfig cacheConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getCacheConfig();
    }

    @Provides
    @Singleton
    public EmailConfig emailConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getEmailConfig();
    }

    @Provides
    @Singleton
    public SegregationConfiguration segregationConfiguration(FoxtrotServerConfiguration configuration) {
        return configuration.getSegregationConfiguration();
    }

    @Provides
    @Singleton
    public ObjectMapper objectMapper(Environment environment) {
        return environment.getObjectMapper();
    }

    @Provides
    @Singleton
    public List<IndexerEventMutator> provideMutators(FoxtrotServerConfiguration configuration,
                                                     ObjectMapper objectMapper) {
        return Collections.singletonList(new LargeTextNodeRemover(objectMapper, configuration.getTextNodeRemover()));
    }

    @Provides
    @Singleton
    public InternalEventBus internalEventBus() {
        return new GuavaInternalEventBus();
    }

    @Provides
    @Singleton
    public EventBus provideEventbus() {
        return new AsyncEventBus(Executors.newCachedThreadPool());
    }

    @Provides
    @Singleton
    public EmailClient emailClient(EmailConfig emailConfig) {
        return new EmailClient(emailConfig);
    }

    @Provides
    @Singleton
    public List<ActionExecutionObserver> actionExecutionObservers(CacheManager cacheManager,
                                                                  InternalEventBus eventBus,
                                                                  QueryConfig queryConfig,
                                                                  EmailConfig emailConfig,
                                                                  EmailClient emailClient,
                                                                  RichEmailBuilder richEmailBuilder,
                                                                  EventBusManager eventBusManager) {
        return ImmutableList.<ActionExecutionObserver>builder()
                .add(new MetricRecorder())
                .add(new ResponseCacheUpdater(cacheManager))
                .add(new SlowQueryReporter(queryConfig))
                .add(new EventPublisherActionExecutionObserver(eventBus, eventBusManager, queryConfig))
                .build();
    }

    @Provides
    @Singleton
    public RichEmailBuilder richEmailBuilder() {
        EmailSubjectBuilder emailSubjectBuilder = new StrSubstitutorEmailSubjectBuilder();
        EmailBodyBuilder emailBodyBuilder = new StrSubstitutorEmailBodyBuilder();
        return new RichEmailBuilder(emailSubjectBuilder, emailBodyBuilder);
    }

    @Provides
    @Singleton
    public ExecutorService provideGlobalExecutorService(Environment environment) {
        return environment.lifecycle()
                .executorService("query-executor-%s")
                .minThreads(20)
                .maxThreads(30)
                .keepAliveTime(Duration.seconds(30))
                .build();
    }

    @Provides
    @Singleton
    public ScheduledExecutorService provideGlobalScheduledExecutorService(Environment environment) {
        return environment.lifecycle()
                .scheduledExecutorService("cardinality-executor")
                .threads(1)
                .build();
    }

    @Provides
    @Singleton
    public FunnelConfiguration funnelConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getFunnelConfiguration() != null
                ? configuration.getFunnelConfiguration()
                : FunnelConfiguration.builder()
                .baseFunnelEventConfig(BaseFunnelEventConfig.builder()
                        .eventType("APP_LOADED")
                        .category("APP_LOADED")
                        .build())
                .querySize(100)
                .build();
    }

    @Provides
    @Singleton
    public Configuration provideHBaseConfiguration(HbaseConfig hbaseConfig) throws IOException {
        return HBaseUtil.create(hbaseConfig);
    }

    @Provides
    @Singleton
    public HazelcastDistributedLockConfig hazelcastDistributedLockConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getDistributedLockConfig();
    }

    @Provides
    @Singleton
    public ElasticsearchTuningConfig provideElasticsearchTuningConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getElasticsearchTuningConfig();
    }

    @Provides
    @Singleton
    public GeojsonStoreConfiguration provideGeoJsonStoreConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getGeojsonStoreConfiguration();
    }

    @Provides
    @Singleton
    public QueryConfig providerQueryConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getQueryConfig();
    }

    @Provides
    @Singleton
    public TranslatorConfig translatorConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getTranslatorConfig();
    }

    @Provides
    @Singleton
    public ServerFactory serverFactory(FoxtrotServerConfiguration configuration) {
        return configuration.getServerFactory();
    }


    @Provides
    @Singleton
    public ShardCountTuningJobConfig shardCountTuningJobConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getShardCountTuningJobConfig();
    }

    @Provides
    @Singleton
    public TableIndexMetadataJobConfig tableIndexMetadataJobConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getTableIndexMetadataJobConfig();
    }

    @Provides
    @Singleton
    public IndexMetadataCleanupJobConfig indexMetadataCleanupJobConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getIndexMetadataCleanupJobConfig();
    }

    @Provides
    @Singleton
    public boolean restrictAccess(FoxtrotServerConfiguration configuration) {
        return configuration.isRestrictAccess();
    }

    @Provides
    @Singleton
    public List<String> restrictedTables(FoxtrotServerConfiguration configuration) {
        return configuration.getRestrictedTables();
    }

    @Provides
    @Singleton
    public NodeGroupActivityConfig provideNodeGroupActivityConfig(FoxtrotServerConfiguration configuration) {
        if (configuration.getNodeGroupActivityConfig() == null) {
            return NodeGroupActivityConfig.builder()
                    .vacantGroupReadRepairIntervalInMins(5)
                    .build();
        }
        return configuration.getNodeGroupActivityConfig();
    }

//    @Provides
//    @Singleton
//    public Authenticator<String, UserPrincipal> provideAuthenticator() {
//        return TokenAuthenticator.class;
//    }
}
