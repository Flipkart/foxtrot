package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class TrendActionTest {

    private static final Logger logger = LoggerFactory.getLogger(TrendActionTest.class.getSimpleName());
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private JsonNodeFactory factory = JsonNodeFactory.instance;

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
                .save(TestUtils.TEST_TABLE, TestUtils.getTrendDocuments(mapper));
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testTrendActionAnyException() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - Any Exception");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(trendRequest);
        logger.info("Tested Trend - Any Exception");
    }

    //TODO trend action with null field is not working
    @Test
    public void testTrendActionNullField() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField(null);

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    //TODO trend action with all field is not working
    @Test
    public void testTrendActionFieldAll() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setField("all");
        trendRequest.setValues(Collections.<String>emptyList());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    @Test(expected = QueryStoreException.class)
    public void testTrendActionNullTable() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(null);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        queryExecutor.execute(trendRequest);
        logger.info("Tested Trend - With Field");
    }

    @Test
    public void testTrendActionWithField() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    @Test
    public void testTrendActionWithFieldZeroTo() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(0L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    @Test
    public void testTrendActionWithFieldZeroFrom() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setTo(0L);
        trendRequest.setField("os");

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1))
                .add(factory.objectNode().put("period", 1397952000000L).put("count", 1))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    @Test
    public void testTrendActionWithFieldWithValues() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field - With Values");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setValues(Arrays.asList("android"));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 6))
                .add(factory.objectNode().put("period", 1398643200000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field - With Values");
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithValues() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field - With Filter - With Values");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setValues(Arrays.asList("android"));

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        List<Filter> filters = new ArrayList<Filter>();
        filters.add(equalsFilter);
        trendRequest.setFilters(filters);

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 2)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field - With Filter - With Values ");
    }

    @Test
    public void testTrendActionWithFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        trendRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397606400000L).put("count", 2)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397692800000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field");
    }

    @Test
    public void testTrendActionWithFieldWithFilterWithInterval() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field - With Interval");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TestUtils.TEST_TABLE);
        trendRequest.setFrom(1L);
        trendRequest.setField("os");
        trendRequest.setTo(System.currentTimeMillis());
        trendRequest.setInterval(1000L * 60 * 60 * 12);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("version");
        equalsFilter.setValue(1);
        trendRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ObjectNode result = factory.objectNode();
        result.put("opcode", "trend");
        ObjectNode trends = factory.objectNode();
        trends.put("android", factory.arrayNode().add(factory.objectNode().put("period", 1397649600000L).put("count", 2)));
        trends.put("ios", factory.arrayNode().add(factory.objectNode().put("period", 1397736000000L).put("count", 1)));
        result.put("trends", trends);

        String expectedResponse = mapper.writeValueAsString(result);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(trendRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Trend - With Field - With Interval");
    }
}
