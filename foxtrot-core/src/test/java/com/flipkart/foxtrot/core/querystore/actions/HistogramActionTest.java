package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class HistogramActionTest {
    private final Logger logger = LoggerFactory.getLogger(FilterActionTest.class.getSimpleName());
    private QueryExecutor queryExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
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
                .save(TestUtils.TEST_TABLE, getHistogramDocuments());
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testHistogramActionAnyException() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Any Exception");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(histogramRequest);
        logger.info("Tested Histogram - Any Exception");
    }

    @Test
    public void testHistogramActionIntervalMinuteNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval minute - No Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397651100000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397658060000L).put("count", 3));
        countsNode.add(factory.objectNode().put("period", 1397658180000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397758200000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397958060000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398653100000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1398658200000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval minute - No Filter");
    }

    @Test
    public void testHistogramActionIntervalMinuteWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval minute - No Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397651100000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397658060000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397658180000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397958060000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398658200000L).put("count", 1));
        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval minute - No Filter");
    }

    @Test
    public void testHistogramActionIntervalHourNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval hour - No Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.hours);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397649600000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397656800000L).put("count", 4));
        countsNode.add(factory.objectNode().put("period", 1397757600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397955600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398650400000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1398657600000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval hour - No Filter");
    }

    @Test
    public void testHistogramActionIntervalHourWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval hour - No Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.hours);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397649600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397656800000L).put("count", 3));
        countsNode.add(factory.objectNode().put("period", 1397955600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398657600000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval hour - No Filter");
    }

    @Test
    public void testHistogramActionIntervalDayNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval Day - No Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.days);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397606400000L).put("count", 6));
        countsNode.add(factory.objectNode().put("period", 1397692800000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397952000000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398643200000L).put("count", 3));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval Day - No Filter");
    }

    @Test
    public void testHistogramActionIntervalDayWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Histogram - Interval Day - With Filter");
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.days);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397606400000L).put("count", 4));
        countsNode.add(factory.objectNode().put("period", 1397952000000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398643200000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
        logger.info("Tested Histogram - Interval Day - With Filter");
    }

    private List<Document> getHistogramDocuments() {
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
