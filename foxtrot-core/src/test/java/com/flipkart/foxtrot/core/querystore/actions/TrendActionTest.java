package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 29/04/14.
 */
public class TrendActionTest {

    private static final Logger logger = LoggerFactory.getLogger(TrendActionTest.class.getSimpleName());
    private static QueryExecutor queryExecutor;
    private static ObjectMapper mapper = new ObjectMapper();
    private static MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer();
    private static HazelcastInstance hazelcastInstance;
    private static String TEST_APP = "test-app";
    private static JsonNodeFactory factory;

    @BeforeClass
    public static void setUp() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();
        factory = JsonNodeFactory.instance;

        //Initializing Cache Factory
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

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
                .save(TEST_APP, getTrendDocuments());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testTrendActionWithField() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TEST_APP);
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
    public void testTrendActionWithFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Trend - With Field");
        TrendRequest trendRequest = new TrendRequest();
        trendRequest.setTable(TEST_APP);
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
        trendRequest.setTable(TEST_APP);
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


    private static List<Document> getTrendDocuments() {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397651117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397651117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658218001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1398658218002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397758218003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397958118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1398653118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1398653118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }
}
