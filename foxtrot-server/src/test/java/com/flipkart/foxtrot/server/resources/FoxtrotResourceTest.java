package com.flipkart.foxtrot.server.resources;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.*;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.persistence.ElasticsearchFunnelStore;
import com.flipkart.foxtrot.core.funnel.services.MappingService;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.pipeline.impl.DistributedPipelineMetadataManager;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutorFactory;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.flipkart.foxtrot.core.tenant.impl.DistributedTenantMetadataManager;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.flipkart.foxtrot.pipeline.PipelineExecutor;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.QueryManager;
import com.flipkart.foxtrot.server.console.QueryManagerImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.dropwizard.Configuration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 27/12/15.
 */
@Slf4j
@Getter
public abstract class FoxtrotResourceTest {

    protected static HazelcastConnection hazelcastConnection;
    protected static ElasticsearchConnection elasticsearchConnection;

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    protected final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    protected final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    protected final LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment();
    protected final Environment environment = mock(Environment.class);
    protected final Bootstrap<FoxtrotServerConfiguration> bootstrap = mock(Bootstrap.class);
    protected final ObjectMapper objectMapper = Jackson.newObjectMapper();
    protected final Configuration configuration = mock(Configuration.class);
    private final ObjectMapper mapper;
    private final HazelcastInstance hazelcastInstance;
    private final TableMetadataManager tableMetadataManager;
    private final TenantMetadataManager tenantMetadataManager;
    private final PipelineMetadataManager pipelineMetadataManager;
    private final CardinalityConfig cardinalityConfig;
    private final List<IndexerEventMutator> mutators;
    private final CacheManager cacheManager;
    private final AnalyticsLoader analyticsLoader;
    private final QueryExecutorFactory queryExecutorFactory;
    private final QueryStore queryStore;
    private final DataStore dataStore;
    private final CardinalityValidator cardinalityValidator;
    private final EmailConfig emailConfig;
    private final EmailClient emailClient;
    private final RichEmailBuilder richEmailBuilder;
    private CardinalityCalculationFactory cardinalityCalculationFactory;
    private CardinalityCalculationService cardinalityCalculationService;
    private ElasticsearchTemplateMappingParser templateMappingParser;
    private QueryManager queryManager;

    @SneakyThrows
    protected FoxtrotResourceTest() throws IOException {
        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.getObjectMapper()).thenReturn(objectMapper);
        when(bootstrap.getObjectMapper()).thenReturn(objectMapper);
        when(environment.getApplicationContext()).thenReturn(new MutableServletContextHandler());
        when(environment.getAdminContext()).thenReturn(new MutableServletContextHandler());
        when(environment.getValidator()).thenReturn(Validators.newValidator());

        environment.getObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        environment.getObjectMapper()
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        environment.getObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapper()
                .setSubtypeResolver(subtypeResolver);

        mapper = environment.getObjectMapper();
        SerDe.init(mapper);
        environment.jersey()
                .register(new FoxtrotExceptionMapper(mapper));

        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        try {
            elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());

        Config config = new Config();
        //Initializing Cache Factory
        hazelcastConnection = mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);

        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));

        cardinalityConfig = new CardinalityConfig("true", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        cardinalityCalculationService = new CardinalityCalculationServiceImpl(cardinalityConfig,
                elasticsearchConnection);
        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, FieldCardinalityMapStore.CARDINALITY_CACHE_INDEX);
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection,
                cardinalityCalculationService, cardinalityConfig);
        tenantMetadataManager = new DistributedTenantMetadataManager(hazelcastConnection, elasticsearchConnection);
        pipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection, elasticsearchConnection);
        try {
            tenantMetadataManager.start();
            tableMetadataManager.start();
            pipelineMetadataManager.start();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(7)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build());
        tenantMetadataManager.save(Tenant.builder()
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .emailIds(new String[]{TestUtils.TEST_EMAIL})
                .build());
        pipelineMetadataManager.save(Pipeline.builder()
                .name(TestUtils.TEST_PIPELINE_NAME)
                .processors(ImmutableList.of(new StringLowerProcessorDefinition()))
                .build());

        mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper, TextNodeRemoverConfiguration.builder()
                .build()));
        dataStore = TestUtils.getDataStore();
        templateMappingParser = new ElasticsearchTemplateMappingParser();
        PipelineUtils.init(environment.getObjectMapper(), ImmutableSet.of("com.flipkart.foxtrot.pipeline.processors"));
        PipelineExecutor pipelineExecutor = new PipelineExecutor(
                new ReflectionBasedProcessorFactory(Guice.createInjector(new PipelineModule())));

        queryStore = new ElasticsearchQueryStore(tableMetadataManager, tenantMetadataManager, elasticsearchConnection, dataStore, mutators, templateMappingParser, cardinalityConfig);

        cardinalityValidator = new CardinalityValidatorImpl(queryStore, tableMetadataManager);
        analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection,
                cacheManager, mapper, new ElasticsearchTuningConfig(), cardinalityValidator);

        emailConfig = new EmailConfig();
        emailClient = Mockito.mock(EmailClient.class);
        richEmailBuilder = Mockito.mock(RichEmailBuilder.class);
        cardinalityCalculationFactory = new CardinalityCalculationFactory(tableMetadataManager,
                cardinalityCalculationService, cardinalityConfig, emailClient, richEmailBuilder, emailConfig);

        queryManager = new QueryManagerImpl(hazelcastConnection);
        try {
            analyticsLoader.start();
            TestUtils.registerActions(analyticsLoader, mapper);
        } catch (Exception e) {
            log.error("Error in initialization", e);
            Assert.fail();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        FunnelConfiguration funnelConfiguration = FunnelConfiguration.builder()
                .querySize(100)
                .baseFunnelEventConfig(BaseFunnelEventConfig.builder()
                        .eventType("APP_LOADED")
                        .category("General")
                        .build())
                .funnelIndex("foxtrot_funnel")
                .build();
        MappingService mappingService = new MappingService(elasticsearchConnection, funnelConfiguration);
        ElasticsearchFunnelStore funnelStore = new ElasticsearchFunnelStore(elasticsearchConnection, mappingService,
                funnelConfiguration);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastConnection.getHazelcast()
                .shutdown();
        elasticsearchConnection.stop();
    }
}
