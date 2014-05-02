package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */

public class FilterActionTest {
    private final Logger logger = LoggerFactory.getLogger(FilterActionTest.class.getSimpleName());
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private String TEST_APP = "test-app";
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
        when(tableMetadataManager.exists(TEST_APP)).thenReturn(true);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor)
                .save(TEST_APP, getQueryDocuments());
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testQueryException() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Any Exception");
        Query query = new Query();
        query.setTable(TEST_APP);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(query);
        logger.info("Tested Query - Any Exception");
    }

    @Test
    public void testQueryNoFilterAscending() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - No Filter - Sort Ascending");
        Query query = new Query();
        query.setTable(TEST_APP);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.asc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Sort Ascending");
    }

    @Test
    public void testQueryNoFilterDescending() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - No Filter - Sort Descending");
        Query query = new Query();
        query.setTable(TEST_APP);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Sort Descending");
    }

    @Test
    public void testQueryNoFilterWithLimit() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - No Filter - Limit 2");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(2);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - No Filter - Limit 2");
    }

    @Test
    public void testQueryAnyFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Any Filter");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        AnyFilter filter = new AnyFilter();
        query.setFilters(Collections.<Filter>singletonList(filter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Any Filter");
    }

    @Test
    public void testQueryEqualsFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - equals Filter");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - equals Filter");
    }

    @Test
    public void testQueryNotEqualsFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - not_equals Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
        notEqualsFilter.setField("os");
        notEqualsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(notEqualsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - not_equals Filter");
    }

    @Test
    public void testQueryGreaterThanFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - greater_than Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_than Filter");
    }

    @Test
    public void testQueryGreaterEqualFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - greater_equal Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(greaterEqualFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_equal Filter");
    }

    @Test
    public void testQueryLessThanFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - less_than Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setField("battery");
        lessThanFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessThanFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - less_than Filter");
    }

    @Test
    public void testQueryLessEqualFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - greater_equal Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        LessEqualFilter lessEqualFilter = new LessEqualFilter();
        lessEqualFilter.setField("battery");
        lessEqualFilter.setValue(48);
        query.setFilters(Collections.<Filter>singletonList(lessEqualFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - greater_equal Filter");
    }

    @Test
    public void testQueryBetweenFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - between Filter");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setLimit(3);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        BetweenFilter betweenFilter = new BetweenFilter();
        betweenFilter.setField("battery");
        betweenFilter.setFrom(47);
        betweenFilter.setTo(75);
        query.setFilters(Collections.<Filter>singletonList(betweenFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - between Filter");
    }

    @Test
    public void testQueryContainsFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - contains Filter");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("os");
        containsFilter.setExpression(".*droid.*");
        query.setFilters(Collections.<Filter>singletonList(containsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - contains Filter");
    }

    @Test
    public void testQueryEmptyResult() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Empty Result Test");
        Query query = new Query();
        query.setTable(TEST_APP);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("wp8");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        ArrayNode arrayNode = factory.arrayNode();
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Empty Result Test");
    }

    @Test
    public void testQueryMultipleFiltersEmptyResult() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Multiple Filters - Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("android");

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(100);

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(greaterEqualFilter);
        query.setFilters(filters);

        ArrayNode arrayNode = factory.arrayNode();
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Empty Result");
    }

    @Test
    public void testQueryMultipleFiltersAndCombiner() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Multiple Filters - Non Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("android");

        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField("battery");
        greaterEqualFilter.setValue(98);

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(greaterEqualFilter);
        query.setFilters(filters);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Non Empty Result");
    }

    @Test
    public void testQueryMultipleFiltersOrCombiner() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Multiple Filters - Or Combiner - Non Empty Result");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");

        EqualsFilter equalsFilter2 = new EqualsFilter();
        equalsFilter2.setField("device");
        equalsFilter2.setValue("nexus");

        List<Filter> filters = new Vector<Filter>();
        filters.add(equalsFilter);
        filters.add(equalsFilter2);
        query.setFilters(filters);

        query.setCombiner(FilterCombinerType.or);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Multiple Filters - Or Combiner - Non Empty Result");
    }

    @Test
    public void testQueryPagination() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Filter with Pagination");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Filter with Pagination");
    }

    @Test
    public void testQueryAsync() throws QueryStoreException, JsonProcessingException, InterruptedException {
        logger.info("Testing Query - Async");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        AsyncDataToken response = queryExecutor.executeAsync(query);
        Thread.sleep(200);
        ActionResponse actionResponse = CacheUtils.getCacheFor(response.getAction()).get(response.getKey());
        String actualResponse = mapper.writeValueAsString(actionResponse);
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Async");
    }

    @Test
    public void testQueryNullFilters() throws QueryStoreException, JsonProcessingException, InterruptedException {
        logger.info("Testing Query - Null Filters");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setFilters(null);
        query.setCombiner(FilterCombinerType.and);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Null Filters");
    }

    @Test
    public void testQueryNullCombiner() throws QueryStoreException, JsonProcessingException, InterruptedException {
        logger.info("Testing Query - Null Combiner");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setFilters(new ArrayList<Filter>());
        query.setCombiner(null);
        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Null Combiner");
    }

    @Test
    public void testQueryNullSort() throws QueryStoreException, JsonProcessingException, InterruptedException {
        logger.info("Testing Query - Null Sort");
        Query query = new Query();
        query.setTable(TEST_APP);
        query.setFilters(new ArrayList<Filter>());
        query.setCombiner(FilterCombinerType.and);
        query.setSort(null);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        arrayNode.addPOJO(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(query));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Query - Null Sort");
    }

    //TODO How to verify if cached data is returned.
    @Test
    public void testQueryCaching() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Query - Query Caching");
        Query query = new Query();
        query.setTable(TEST_APP);

        ResultSort resultSort = new ResultSort();
        resultSort.setOrder(ResultSort.Order.desc);
        resultSort.setField("_timestamp");
        query.setSort(resultSort);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("os");
        equalsFilter.setValue("ios");
        query.setFilters(Collections.<Filter>singletonList(equalsFilter));

        query.setFrom(1);
        query.setLimit(1);

        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.addPOJO(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        ObjectNode expectedResponseNode = factory.objectNode();
        expectedResponseNode.put("opcode", "query");
        expectedResponseNode.put("documents", arrayNode);
        String expectedResponse = mapper.writeValueAsString(expectedResponseNode);

        assertEquals(expectedResponse, mapper.writeValueAsString(queryExecutor.execute(query)));
        assertEquals(expectedResponse, mapper.writeValueAsString(queryExecutor.execute(query)));
        logger.info("Tested Query - Query Caching");
    }

    private List<Document> getQueryDocuments() {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 1, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 1, "device", "galaxy"}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus"}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone"}, mapper));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad"}, mapper));
        return documents;
    }
}
