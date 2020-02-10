package com.flipkart.foxtrot.core.querystore.actions;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public abstract class ActionTest {

    protected static final int MAX_CARDINALITY = 15000;

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

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
    private static DistributedTableMetadataManager tableMetadataManager;
    @Getter
    private static CacheManager cacheManager;

    @Getter
    private static HbaseTableConnection tableConnection;

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setHost("127.0.0.1");
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(hazelcastInstance.getConfig());
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper, cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder()
                                          .name(TestUtils.TEST_TABLE_NAME)
                                          .ttl(30)
                                          .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper,
                                         TextNodeRemoverConfiguration.builder().build()));
        tableConnection = Mockito.mock(HbaseTableConnection.class);
        DataStore dataStore = TestUtils.getDataStore(tableConnection);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mutators, mapper, cardinalityConfig);
        cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager,
                                                              dataStore,
                                                              queryStore,
                                                              elasticsearchConnection,
                                                              cacheManager,
                                                              mapper,
                                                              new ElasticsearchTuningConfig());
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService, Collections.singletonList(new ResponseCacheUpdater(cacheManager)));
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
