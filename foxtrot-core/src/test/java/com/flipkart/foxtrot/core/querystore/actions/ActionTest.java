package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
public class ActionTest {

    private ObjectMapper mapper;
    private QueryStore queryStore;
    private QueryExecutor queryExecutor;
    private HazelcastInstance hazelcastInstance;
    private MockElasticsearchServer elasticsearchServer;
    private ElasticsearchConnection elasticsearchConnection;

    @Before
    public void setUp() throws Exception {
        this.mapper = new ObjectMapper();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        this.hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);

        this.elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        elasticsearchServer = spy(elasticsearchServer);
        this.elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        doReturn(elasticsearchServer.getClient()).when(elasticsearchConnection).getClient();
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        doReturn(true).when(tableMetadataManager).exists(TestUtils.TEST_TABLE_NAME);
        doReturn(TestUtils.TEST_TABLE).when(tableMetadataManager).get(anyString());

        DataStore dataStore = TestUtils.getDataStore();
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper));
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

    public ObjectMapper getMapper() {
        return mapper;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public MockElasticsearchServer getElasticsearchServer() {
        return elasticsearchServer;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public ElasticsearchConnection getElasticsearchConnection() {
        return elasticsearchConnection;
    }
}
