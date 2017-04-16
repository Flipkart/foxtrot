package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.top.TopNParams;
import com.flipkart.foxtrot.common.top.TopNRequest;
import com.flipkart.foxtrot.common.top.TopNResponse;
import com.flipkart.foxtrot.common.top.ValueCount;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TopNActionTest {

    private QueryExecutor queryExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
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
        List<Document> documents = TestUtils.getTopNDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testTopNSingleField() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("os", false, 2));
        request.setParams(params);

        TopNResponse response = TopNResponse.class.cast(queryExecutor.execute(request));
        assert(response != null);
        assert(response.getData() != null);
        assert(response.getData().size() == 1);
        assert(response.getData().get("os") != null);
        assert(response.getData().get("os").size() == 2);
        List<ValueCount> counts = response.getData().get("os");

        assertEquals("android", counts.get(0).getValue());
        assertEquals(7, counts.get(0).getCount());

        assertEquals("ios", counts.get(1).getValue());
        assertEquals(4, counts.get(1).getCount());
    }

    @Test
    public void testTopNSingleFieldWithFilter() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        request.setFilters(Arrays.<Filter>asList(equalsFilter));

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("os", false, 2));
        request.setParams(params);

        TopNResponse response = TopNResponse.class.cast(queryExecutor.execute(request));
        assert(response != null);
        assert(response.getData() != null);
        assert(response.getData().size() == 1);
        assert(response.getData().get("os") != null);
        assert(response.getData().get("os").size() == 2);
        List<ValueCount> counts = response.getData().get("os");

        assertEquals("android", counts.get(0).getValue());
        assertEquals(2, counts.get(0).getCount());

        assertEquals("ios", counts.get(1).getValue());
        assertEquals(1, counts.get(1).getCount());
    }



    @Test
    public void testTopNSingleFieldCountLessThanRequested() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("test", false, 2));
        request.setParams(params);

        TopNResponse response = TopNResponse.class.cast(queryExecutor.execute(request));
        assert (response != null);
        assert (response.getData() != null);
        assert (response.getData().size() == 1);
        assert (response.getData().get("test") != null);
        assert (response.getData().get("test").size() == 1);
        List<ValueCount> counts = response.getData().get("test");

        assertEquals("ABCD", counts.get(0).getValue());
        assertEquals(1, counts.get(0).getCount());
    }

    @Test
    public void testTopNMultipleFields() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("os", false, 2));
        params.add(new TopNParams("version", false, 2));
        request.setParams(params);

        TopNResponse response = TopNResponse.class.cast(queryExecutor.execute(request));
        assert(response != null);
        assert(response.getData() != null);
        assert(response.getData().size() == 2);

        assert(response.getData().get("os") != null);
        assert(response.getData().get("os").size() == 2);
        List<ValueCount> counts = response.getData().get("os");

        assertEquals("android", counts.get(0).getValue());
        assertEquals(7, counts.get(0).getCount());

        assertEquals("ios", counts.get(1).getValue());
        assertEquals(4, counts.get(1).getCount());

        assert(response.getData().get("version") != null);
        assert(response.getData().get("version").size() == 2);
        counts = response.getData().get("version");

        assertEquals("2", counts.get(0).getValue());
        assertEquals(10, counts.get(0).getCount());

        assertEquals("1", counts.get(1).getValue());
        assertEquals(4, counts.get(1).getCount());
    }

    @Test
    public void testTopNNegativeCount() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("os", false, -1));
        request.setParams(params);
        try {
            queryExecutor.execute(request);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTopNZeroCount() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);

        List<TopNParams> params = new ArrayList<TopNParams>();
        params.add(new TopNParams("os", false, 0));
        request.setParams(params);
        try {
            queryExecutor.execute(request);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTopNNullTable() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(null);

        try {
            queryExecutor.execute(request);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTopNNullFilters() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setFilters(null);

        try {
            queryExecutor.execute(request);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testTopNNullParams() throws QueryStoreException {
        TopNRequest request = new TopNRequest();
        request.setTable(TestUtils.TEST_TABLE_NAME);
        request.setParams(null);

        try {
            queryExecutor.execute(request);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }
}