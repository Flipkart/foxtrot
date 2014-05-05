package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest {

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
                .save(TestUtils.TEST_TABLE, getGroupDocuments());
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testGroupActionSingleQueryException() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Single Field - Any Exception");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os"));
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(groupRequest);
        logger.info("Tested Group - Single Field - Any Exception");
    }

    @Test
    public void testGroupActionSingleFieldNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Single Field - No Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", 7);
        resultNode.put("ios", 4);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Single Field - No Filter");
    }

    @Test
    public void testGroupActionSingleFieldWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Single Field - With Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Arrays.asList("os"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", 5);
        resultNode.put("ios", 1);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Single Field - With Filter");
    }

    @Test
    public void testGroupActionTwoFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Single Field - No Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", factory.objectNode().put("nexus", 5).put("galaxy", 2));
        resultNode.put("ios", factory.objectNode().put("nexus", 1).put("ipad", 2).put("iphone", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Single Field - No Filter");
    }

    @Test
    public void testGroupActionTwoFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Two Fields - With Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ObjectNode resultNode = factory.objectNode();
        resultNode.put("android", factory.objectNode().put("nexus", 3).put("galaxy", 2));
        resultNode.put("ios", factory.objectNode().put("ipad", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);
        String expectedResult = mapper.writeValueAsString(finalNode);

        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Two Fields - With Filter");
    }

    @Test
    public void testGroupActionMultipleFieldsNoFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Multiple Fields - With Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2).put("1", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("2", 1));
        temp.put("ipad", factory.objectNode().put("2", 2));
        temp.put("iphone", factory.objectNode().put("1", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Multiple Fields - With Filter");
    }

    @Test
    public void testGroupActionMultipleFieldsWithFilter() throws QueryStoreException, JsonProcessingException {
        logger.info("Testing Group - Multiple Fields - With Filter");
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ObjectNode resultNode = factory.objectNode();

        ObjectNode temp = factory.objectNode();
        temp.put("nexus", factory.objectNode().put("3", 1).put("2", 2));
        temp.put("galaxy", factory.objectNode().put("3", 1).put("2", 1));
        resultNode.put("android", temp);

        temp = factory.objectNode();
        temp.put("ipad", factory.objectNode().put("2", 1));
        resultNode.put("ios", temp);

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "group");
        finalNode.put("result", resultNode);

        String expectedResult = mapper.writeValueAsString(finalNode);
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
        logger.info("Tested Group - Multiple Fields - With Filter");
    }

    private List<Document> getGroupDocuments() {
        List<Document> documents = new Vector<Document>();
        documents.add(TestUtils.getDocument("Z", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("Y", 1397658117000L, new Object[]{"os", "android", "version", 1, "device", "nexus", "battery", 48}, mapper));
        documents.add(TestUtils.getDocument("X", 1397658117000L, new Object[]{"os", "android", "version", 3, "device", "galaxy", "battery", 74}, mapper));
        documents.add(TestUtils.getDocument("W", 1397658117000L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 99}, mapper));
        documents.add(TestUtils.getDocument("A", 1397658118000L, new Object[]{"os", "android", "version", 3, "device", "nexus", "battery", 87}, mapper));
        documents.add(TestUtils.getDocument("B", 1397658118001L, new Object[]{"os", "android", "version", 2, "device", "galaxy", "battery", 76}, mapper));
        documents.add(TestUtils.getDocument("C", 1397658118002L, new Object[]{"os", "android", "version", 2, "device", "nexus", "battery", 78}, mapper));
        documents.add(TestUtils.getDocument("D", 1397658118003L, new Object[]{"os", "ios", "version", 1, "device", "iphone", "battery", 24}, mapper));
        documents.add(TestUtils.getDocument("E", 1397658118004L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 56}, mapper));
        documents.add(TestUtils.getDocument("F", 1397658118005L, new Object[]{"os", "ios", "version", 2, "device", "nexus", "battery", 35}, mapper));
        documents.add(TestUtils.getDocument("G", 1397658118006L, new Object[]{"os", "ios", "version", 2, "device", "ipad", "battery", 44}, mapper));
        return documents;
    }
}
