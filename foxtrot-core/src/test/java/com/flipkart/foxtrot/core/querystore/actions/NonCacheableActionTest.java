package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.common.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
public class NonCacheableActionTest {

    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;

    @Before
    public void setUp() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        // Ensure that table exists before saving/reading data from it
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE)).thenReturn(true);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor)
                .save(TestUtils.TEST_TABLE, TestUtils.getQueryDocuments(mapper));
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    //TODO how to verify if cache is hit or not ?
    @Test
    public void checkCacheability() throws QueryStoreException {
        queryExecutor.execute(new NonCacheableActionRequest());
    }
}
