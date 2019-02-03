package com.flipkart.foxtrot.core.querystore.actions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.Getter;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
@Getter
public class ActionTest {

    private ObjectMapper mapper;
    private QueryStore queryStore;
    private QueryExecutor queryExecutor;
    private HazelcastInstance hazelcastInstance;
    private MockElasticsearchServer elasticsearchServer;
    private ElasticsearchConnection elasticsearchConnection;
    private DistributedTableMetadataManager tableMetadataManager;

    static {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);
    }

    @Before
    public void setUp() throws Exception {
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Kolkata"));
        this.mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        Config config = new Config();
        this.hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);
        this.elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        elasticsearchConnection = TestUtils.initESConnection(elasticsearchServer);
        CardinalityConfig cardinalityConfig = new CardinalityConfig("true", String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));

        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection, mapper, cardinalityConfig);
        tableMetadataManager.start();

        tableMetadataManager.save(Table.builder().name(TestUtils.TEST_TABLE_NAME).ttl(30).build());

        DataStore dataStore = TestUtils.getDataStore();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper, cardinalityConfig);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager,
                dataStore,
                queryStore,
                elasticsearchConnection,
                cacheManager,
                mapper);
        analyticsLoader.start();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService);
    }

    @After
    public void tearDown() throws Exception {
        getElasticsearchServer().shutdown();
        getHazelcastInstance().shutdown();
    }
}
