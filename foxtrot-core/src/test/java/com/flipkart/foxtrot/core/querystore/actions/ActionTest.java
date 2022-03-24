package com.flipkart.foxtrot.core.querystore.actions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.*;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.pipeline.impl.DistributedPipelineMetadataManager;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutor;
import com.flipkart.foxtrot.core.queryexecutor.SimpleQueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.core.tenant.impl.DistributedTenantMetadataManager;
import com.flipkart.foxtrot.pipeline.PipelineExecutor;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.foxtrot.pipeline.di.PipelineModule;
import com.flipkart.foxtrot.pipeline.processors.factory.ReflectionBasedProcessorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.Getter;
import lombok.val;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public abstract class ActionTest {

    protected static final int MAX_CARDINALITY = 15000;
    protected static final String TEST_TENANT = "test-tenant";
    @Getter
    protected static DistributedTableMetadataManager tableMetadataManager;
    @Getter
    protected static DistributedTenantMetadataManager tenantMetadataManager;
    private static HazelcastInstance hazelcastInstance;
    @Getter
    private static ElasticsearchConnection elasticsearchConnection;
    @Getter
    private static ObjectMapper mapper;
    @Getter
    private static QueryStore queryStore;
    @Getter
    private static QueryExecutor queryExecutor;
    @Getter
    private static CacheManager cacheManager;
    @Getter
    private static HbaseTableConnection tableConnection;
    @Getter
    private static CardinalityValidator cardinalityValidator;

    @Getter
    private static ElasticsearchTemplateMappingParser templateMappingParser;


    @Getter
    private static CardinalityCalculationService cardinalityCalculationService;

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        mapper = new ObjectMapper();
        SerDe.init(mapper);
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(hazelcastInstance.getConfig());
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true",
                String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, FieldCardinalityMapStore.CARDINALITY_CACHE_INDEX);
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());

        cardinalityCalculationService = new CardinalityCalculationServiceImpl(cardinalityConfig,
                elasticsearchConnection);
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection,
                cardinalityCalculationService, cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(30)
                .tenantName(TestUtils.TEST_TENANT_NAME)
                .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper,
                TextNodeRemoverConfiguration.builder()
                        .build()));
        tableConnection = Mockito.mock(HbaseTableConnection.class);
        DataStore dataStore = TestUtils.getDataStore(tableConnection);
        val pipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        pipelineMetadataManager.start();
        templateMappingParser = new ElasticsearchTemplateMappingParser();
        PipelineUtils.init(mapper, ImmutableSet.of("com.flipkart.foxtrot.pipeline"));
        PipelineExecutor pipelineExecutor = new PipelineExecutor(
                new ReflectionBasedProcessorFactory(Guice.createInjector(new PipelineModule())));

        queryStore = new ElasticsearchQueryStore(tableMetadataManager, tenantMetadataManager, elasticsearchConnection, dataStore, mutators, templateMappingParser, cardinalityConfig);

        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));

        cardinalityValidator = new CardinalityValidatorImpl(queryStore, tableMetadataManager);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore,
                elasticsearchConnection, cacheManager, mapper, new ElasticsearchTuningConfig(), cardinalityValidator);
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        queryExecutor = new SimpleQueryExecutor(analyticsLoader, executorService,
                ImmutableList.of(new ResponseCacheUpdater(cacheManager),
                        new SlowQueryReporter(new QueryConfig())));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
        elasticsearchConnection.stop();

    }

    @Before
    public void setup() throws Exception {
        doReturn(MockHTable.create()).when(getTableConnection())
                .getTable(Matchers.any());
    }

}
