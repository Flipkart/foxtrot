/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class ElasticsearchQueryStoreTest {
    private MockElasticsearchServer elasticsearchServer;
    private DataStore dataStore;
    private ElasticsearchQueryStore queryStore;
    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        ElasticsearchUtils.setMapper(mapper);
        dataStore = TestUtils.getDataStore();

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        QueryExecutor queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void testSaveSingle() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        queryStore.save(TestUtils.TEST_TABLE_NAME, expectedDocument);

        GetResponse getResponse = elasticsearchServer
                .getClient()
                .prepareGet(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, expectedDocument.getTimestamp()),
                        ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        expectedDocument.getId())
                .setFields("_timestamp").execute().actionGet();
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", expectedDocument.getId(), getResponse.getId());
        assertEquals("Timestamp should match request timestamp", expectedDocument.getTimestamp(), getResponse.getField("_timestamp").getValue());
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveNullId() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(null);
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);

        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, expectedDocument);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveNullData() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        expectedDocument.setData(null);

        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, expectedDocument);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testSaveSingleInvalidTable() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        try {
            queryStore.save(TestUtils.TEST_TABLE + "-missing", expectedDocument);
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, qse.getErrorCode());
        }
    }

    @Test
    public void testSaveBulk() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);

        for (Document document : documents) {
            GetResponse getResponse = elasticsearchServer
                    .getClient()
                    .prepareGet(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()),
                            ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                            document.getId())
                    .setFields("_timestamp").execute().actionGet();
            assertTrue("Id should exist in ES", getResponse.isExists());
            assertEquals("Id should match requestId", document.getId(), getResponse.getId());
            assertEquals("Timestamp should match request timestamp", document.getTimestamp(), getResponse.getField("_timestamp").getValue());
        }
    }

    @Test
    public void testSaveBulkNullList() throws Exception {
        List<Document> list = null;
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, list);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveBulkNullListItem() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(null);
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveBulkNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(null, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testSaveBulkNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testSaveBulkEmptyList() throws Exception {
        List<Document> list = new Vector<Document>();
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, list);
            fail();
        } catch (QueryStoreException ex) {
            assertEquals(QueryStoreException.ErrorCode.INVALID_REQUEST, ex.getErrorCode());
        }
    }

    @Test
    public void testSaveBulkInvalidTable() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        try {
            queryStore.save(TestUtils.TEST_TABLE + "-missing", documents);
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, qse.getErrorCode());
        }
    }

    @Test
    public void testGetSingle() throws Exception {
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document document = new Document(id, System.currentTimeMillis(), data);
        document.setTimestamp(timestamp);
        dataStore.save(TestUtils.TEST_TABLE, document);

        Document responseDocument = queryStore.get(TestUtils.TEST_TABLE_NAME, id);
        assertNotNull(responseDocument);
        assertEquals(id, responseDocument.getId());
        assertEquals("Timestamp should match request timestamp", document.getTimestamp(), responseDocument.getTimestamp());
        Map<String, Object> expectedMap = mapper.convertValue(document.getData(), new TypeReference<Object>() {});
        Map<String, Object> actualMap = mapper.convertValue(responseDocument.getData(), new TypeReference<HashMap<String, Object>>() {});
        assertEquals("Actual data should match expected data", expectedMap, actualMap);
    }

    @Test
    public void testGetSingleInvalidId() throws Exception {
        try {
            queryStore.get(TestUtils.TEST_TABLE_NAME, UUID.randomUUID().toString());
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, qse.getErrorCode());
        }
    }

    @Test(expected = QueryStoreException.class)
    public void testGetSingleInvalidTable() throws Exception {
        try {
            queryStore.get(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testGetBulk() throws Exception {
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
            dataStore.save(TestUtils.TEST_TABLE, document);
        }

        List<Document> responseDocuments = queryStore.getAll(TestUtils.TEST_TABLE_NAME, ids);
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
    }

    @Test
    public void testGetBulkInvalidIds() throws Exception {
        try {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
            dataStore.save(TestUtils.TEST_TABLE, document);
            queryStore.getAll(TestUtils.TEST_TABLE_NAME, Arrays.asList(UUID.randomUUID().toString(), id));
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, qse.getErrorCode());
        }
    }

    @Test
    public void testGetBulkNoIds() throws Exception {
        List<Document> documents = queryStore.getAll(TestUtils.TEST_TABLE_NAME, new ArrayList<String>());
        assertEquals(0, documents.size());
    }

    @Test(expected = QueryStoreException.class)
    public void testGetBulkInvalidTable() throws Exception {
        try {
            queryStore.getAll(UUID.randomUUID().toString(), Arrays.asList("a", "b"));
        } catch (QueryStoreException e) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testGetFieldMappings() throws QueryStoreException, InterruptedException {
        queryStore.save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments(mapper));
        Thread.sleep(500);

        Set<FieldTypeMapping> mappings = new HashSet<FieldTypeMapping>();
        mappings.add(new FieldTypeMapping("word", FieldType.STRING));
        mappings.add(new FieldTypeMapping("data.data", FieldType.STRING));
        mappings.add(new FieldTypeMapping("header.hello", FieldType.STRING));
        mappings.add(new FieldTypeMapping("head.hello", FieldType.LONG));

        TableFieldMapping tableFieldMapping = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, mappings);
        TableFieldMapping responseMapping = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);

        assertEquals(tableFieldMapping.getTable(), responseMapping.getTable());
        assertTrue(tableFieldMapping.getMappings().equals(responseMapping.getMappings()));
    }

    @Test
    public void testGetFieldMappingsNonExistingTable() throws QueryStoreException {
        try {
            queryStore.getFieldMappings(TestUtils.TEST_TABLE + "-test");
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, qse.getErrorCode());
        }
    }

    @Test
    public void testGetFieldMappingsNoDocumentsInTable() throws QueryStoreException {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<FieldTypeMapping>());
        TableFieldMapping response = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);

        assertEquals(request.getTable(), response.getTable());
        assertTrue(request.getMappings().equals(response.getMappings()));
    }
}
