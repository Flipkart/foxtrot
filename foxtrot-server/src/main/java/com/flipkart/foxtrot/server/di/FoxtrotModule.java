package com.flipkart.foxtrot.server.di;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.alerts.AlertingSystemEventConsumer;
import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.EmailSubjectBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.funnel.config.BaseEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.persistence.ElasticsearchFunnelStore;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.funnel.services.EventProcessingService;
import com.flipkart.foxtrot.core.funnel.services.EventProcessingServiceImpl;
import com.flipkart.foxtrot.core.funnel.services.FunnelService;
import com.flipkart.foxtrot.core.funnel.services.FunnelServiceImplV1;
import com.flipkart.foxtrot.core.funnel.services.FunnelServiceImplV2;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.lock.DistributedLock;
import com.flipkart.foxtrot.core.lock.HazelcastDistributedLock;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.config.SegregationConfiguration;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

import java.io.IOException;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.conf.Configuration;


/**
 *
 */
public class FoxtrotModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TableMetadataManager.class)
                .to(DistributedTableMetadataManager.class);
        bind(DataStore.class)
                .to(HBaseDataStore.class);
        bind(QueryStore.class)
                .to(ElasticsearchQueryStore.class);
        bind(FqlStoreService.class)
                .to(FqlStoreServiceImpl.class);
        bind(CacheFactory.class)
                .to(DistributedCacheFactory.class);
        bind(InternalEventBus.class)
                .to(GuavaInternalEventBus.class);
        bind(InternalEventBusConsumer.class)
                .to(AlertingSystemEventConsumer.class);
        bind(ConsolePersistence.class)
                .to(ElasticsearchConsolePersistence.class);
        bind(EmailSubjectBuilder.class)
                .to(StrSubstitutorEmailSubjectBuilder.class);
        bind(EmailBodyBuilder.class)
                .to(StrSubstitutorEmailBodyBuilder.class);
        bind(TableManager.class)
                .to(FoxtrotTableManager.class);
        bind(FunnelService.class).annotatedWith(Names.named("FunnelServiceImplV1")).to(FunnelServiceImplV1.class);
        bind(FunnelService.class).to(FunnelServiceImplV2.class);
        bind(FunnelStore.class).to(ElasticsearchFunnelStore.class);
        bind(EventProcessingService.class).to(EventProcessingServiceImpl.class);
        bind(DistributedLock.class).to(HazelcastDistributedLock.class);
        bind(new TypeLiteral<List<HealthCheck>>() {
        }).toProvider(HealthcheckListProvider.class);
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
    public List<IndexerEventMutator> provideMutators(
            FoxtrotServerConfiguration configuration,
            ObjectMapper objectMapper) {
        return Collections.singletonList(new LargeTextNodeRemover(objectMapper, configuration.getTextNodeRemover()));
    }

    @Provides
    @Singleton
    public List<ActionExecutionObserver> actionExecutionObservers(CacheManager cacheManager,
            InternalEventBus eventBus) {
        return ImmutableList.<ActionExecutionObserver>builder()
                .add(new MetricRecorder())
                .add(new ResponseCacheUpdater(cacheManager))
                .add(new SlowQueryReporter())
                .add(new EventPublisherActionExecutionObserver(eventBus))
                .build();
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
                : new FunnelConfiguration();
    }

    @Provides
    @Singleton
    public FunnelDropdownConfig funnelDropdownConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getFunnelDropdownConfig();
    }

    @Provides
    @Singleton
    public BaseEventConfig provideBaseEventConfig(FoxtrotServerConfiguration configuration) throws IOException {
        return configuration.getBaseEventConfig() != null
                ? configuration.getBaseEventConfig()
                : new BaseEventConfig();
    }

    @Provides
    @Singleton
    public Configuration provideHBaseConfiguration(HbaseConfig hbaseConfig) throws IOException {
        return HBaseUtil.create(hbaseConfig);
    }

    @Provides
    @Singleton
    public ServerFactory serverFactory(FoxtrotServerConfiguration configuration) {
        return configuration.getServerFactory();
    }
}
