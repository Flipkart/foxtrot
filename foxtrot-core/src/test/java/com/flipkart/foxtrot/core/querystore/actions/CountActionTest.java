package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class CountActionTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private QueryExecutor queryExecutor;
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
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        List<Document> documents = TestUtils.getCountDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testCount() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(11, countResponse.getCount());
    }

    @Test
    public void testCountWithFilter() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    @Test
    public void testCountDistinct() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilter() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("device", "nexus"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilterOnSameField() throws FoxtrotException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));
        assertNotNull(countResponse);
        assertEquals(1, countResponse.getCount());
    }


}