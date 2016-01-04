package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.query.ResultSort;
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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class DistinctActionTest {

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
        List<Document> documents = TestUtils.getDistinctDocuments(mapper);

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
    public void testDistinctAsc() throws FoxtrotException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        distinctRequest.setNesting(Arrays.asList(resultSort));

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1"));
        listResponse.add(Arrays.asList("2"));
        listResponse.add(Arrays.asList("3"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(queryExecutor.execute(distinctRequest));
        assertNotNull(distinctResponse);
        assertEquals(expectedResponse, distinctResponse);
    }

    @Test
    public void testDistinctDesc() throws FoxtrotException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);
        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.desc);
        distinctRequest.setNesting(Arrays.asList(resultSort));

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("3"));
        listResponse.add(Arrays.asList("2"));
        listResponse.add(Arrays.asList("1"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(queryExecutor.execute(distinctRequest));
        assertNotNull(distinctResponse);
        assertEquals(expectedResponse, distinctResponse);
    }

    @Test
    public void testDistinctMultipleNestingAscAsc() throws FoxtrotException, JsonProcessingException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);

        List<ResultSort> resultSorts = new ArrayList<ResultSort>();

        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        resultSort = new ResultSort();
        resultSort.setField("os");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        distinctRequest.setNesting(resultSorts);

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version", "os"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1", "android"));
        listResponse.add(Arrays.asList("1", "ios"));
        listResponse.add(Arrays.asList("2", "ios"));
        listResponse.add(Arrays.asList("3", "android"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(queryExecutor.execute(distinctRequest));
        assertNotNull(distinctResponse);
    }

    @Test
    public void testDistinctMultipleNestingAscDesc() throws FoxtrotException, JsonProcessingException {
        DistinctRequest distinctRequest = new DistinctRequest();
        distinctRequest.setTable(TestUtils.TEST_TABLE_NAME);

        List<ResultSort> resultSorts = new ArrayList<ResultSort>();

        ResultSort resultSort = new ResultSort();
        resultSort.setField("version");
        resultSort.setOrder(ResultSort.Order.asc);
        resultSorts.add(resultSort);

        resultSort = new ResultSort();
        resultSort.setField("os");
        resultSort.setOrder(ResultSort.Order.desc);
        resultSorts.add(resultSort);

        distinctRequest.setNesting(resultSorts);

        DistinctResponse expectedResponse = new DistinctResponse();
        expectedResponse.setHeaders(Arrays.asList("version", "os"));

        List<List<String>> listResponse = new ArrayList<List<String>>();
        listResponse.add(Arrays.asList("1", "ios"));
        listResponse.add(Arrays.asList("1", "android"));
        listResponse.add(Arrays.asList("2", "ios"));
        listResponse.add(Arrays.asList("3", "android"));
        expectedResponse.setResult(listResponse);

        DistinctResponse distinctResponse = DistinctResponse.class.cast(queryExecutor.execute(distinctRequest));
        assertNotNull(distinctResponse);
    }
}