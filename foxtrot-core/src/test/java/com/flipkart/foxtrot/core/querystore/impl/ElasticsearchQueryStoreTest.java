package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class ElasticsearchQueryStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStoreTest.class.getSimpleName());
    MockElasticsearchServer elasticsearchServer;
    DataStore dataStore;
    ElasticsearchQueryStore queryStore;
    ObjectMapper mapper;
    String TEST_APP = "test-app";

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        ElasticsearchUtils.setMapper(mapper);

        elasticsearchServer = new MockElasticsearchServer();
        dataStore = TestUtils.getDataStore();
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        QueryExecutor queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TEST_APP)).thenReturn(true);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void testSaveSingle() throws Exception {
        logger.info("Testing Single Save");
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        queryStore.save(TEST_APP, expectedDocument);

        GetResponse getResponse = elasticsearchServer
                .getClient()
                .prepareGet(ElasticsearchUtils.getCurrentIndex(TEST_APP, expectedDocument.getTimestamp()),
                        ElasticsearchUtils.TYPE_NAME,
                        expectedDocument.getId())
                .setFields("_timestamp").execute().actionGet();
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", expectedDocument.getId(), getResponse.getId());
        assertEquals("Timestamp should match request timestamp", expectedDocument.getTimestamp(), getResponse.getField("_timestamp").getValue());
        logger.info("Tested Single Save");
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveSingleInvalidTable() throws Exception {
        logger.info("Testing Single Save - Invalid Table");
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        queryStore.save(TEST_APP + "-missing", expectedDocument);
        logger.info("Tested Single Save - Invalid Table");
    }

    @Test
    public void testSaveBulk() throws Exception {
        logger.info("Testing Bulk Save");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TEST_APP, documents);

        for (Document document : documents) {
            GetResponse getResponse = elasticsearchServer
                    .getClient()
                    .prepareGet(ElasticsearchUtils.getCurrentIndex(TEST_APP, document.getTimestamp()),
                            ElasticsearchUtils.TYPE_NAME,
                            document.getId())
                    .setFields("_timestamp").execute().actionGet();
            assertTrue("Id should exist in ES", getResponse.isExists());
            assertEquals("Id should match requestId", document.getId(), getResponse.getId());
            assertEquals("Timestamp should match request timestamp", document.getTimestamp(), getResponse.getField("_timestamp").getValue());
        }
        logger.info("Tested Bulk Save");
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveBulkInvalidTable() throws Exception {
        logger.info("Testing Bulk Save - Invalid Table");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TEST_APP + "-missing", documents);
        logger.info("Tested Bulk Save - Invalid Table");
    }

    @Test
    public void testGetSingle() throws Exception {
        logger.info("Testing Single Get");
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document document = new Document(id, System.currentTimeMillis(), data);
        document.setTimestamp(timestamp);
        dataStore.save(TEST_APP, document);

        Document responseDocument = queryStore.get(TEST_APP, id);
        assertNotNull(responseDocument);
        assertEquals(id, responseDocument.getId());
        assertEquals("Timestamp should match request timestamp", document.getTimestamp(), responseDocument.getTimestamp());
        logger.info("Tested Single Get");
    }

    @Test(expected = QueryStoreException.class)
    public void testGetSingleInvalidId() throws Exception {
        logger.info("Testing Single Get - Invalid Id");
        queryStore.get(TEST_APP, UUID.randomUUID().toString());
        logger.info("Tested Single Get - Invalid Id");
    }

    @Test
    public void testGetBulk() throws Exception {
        logger.info("Testing Bulk Get");
        Map<String, Document> idValues = new HashMap<String, Document>();
        List<String> ids = new Vector<String>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            idValues.put(id,
                    new Document(id,
                            System.currentTimeMillis(),
                            mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
            idValues.get(id).setTimestamp(System.currentTimeMillis());
        }
        for (Document document : idValues.values()) {
            dataStore.save(TEST_APP, document);
        }

        List<Document> responseDocuments = queryStore.get(TEST_APP, ids);
        HashMap<String, Document> responseIdValues = new HashMap<String, Document>();
        for (Document doc : responseDocuments) {
            responseIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", responseDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", responseIdValues.containsKey(id));
            assertNotNull(responseIdValues.get(id));
            assertEquals(id, responseIdValues.get(id).getId());
            assertEquals("Timestamp should match request timestamp", idValues.get(id).getTimestamp(), responseIdValues.get(id).getTimestamp());
        }
        logger.info("Tested Bulk Get");
    }

    @Test(expected = QueryStoreException.class)
    public void testGetBulkInvalidIds() throws Exception {
        logger.info("Testing Bulk Get - Invalid Ids");
        queryStore.get(TEST_APP, Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        logger.info("Tested Bulk Get - Invalid Ids");
    }
}
