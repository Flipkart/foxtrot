package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.common.NonCacheableAction;
import com.flipkart.foxtrot.core.common.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.common.RequestWithNoAction;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
public class QueryExecutorTest {
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private AnalyticsLoader analyticsLoader;

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

        analyticsLoader = spy(new AnalyticsLoader(dataStore, elasticsearchConnection));
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testResolve() throws Exception {
        assertEquals(NonCacheableAction.class, queryExecutor.resolve(new NonCacheableActionRequest()).getClass());
    }

    @Test(expected = QueryStoreException.class)
    public void testResolveNonExistentAction() throws Exception {
        queryExecutor.resolve(new RequestWithNoAction());
    }

    @Test(expected = QueryStoreException.class)
    public void testResolveLoaderException() throws Exception {
        doThrow(new IOException()).when(analyticsLoader).getAction(any(ActionRequest.class));
        queryExecutor.resolve(new NonCacheableActionRequest());
    }
}
