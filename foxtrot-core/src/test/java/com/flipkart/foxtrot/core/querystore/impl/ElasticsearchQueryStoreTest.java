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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.FieldData;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.TableFieldMetadata;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getMapper;
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

    @Before
    public void setUp() throws Exception {
        ElasticsearchUtils.setMapper(new ObjectMapper());
        dataStore = TestUtils.getDataStore();
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
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
        JsonNode data = getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        queryStore.save(TestUtils.TEST_TABLE_NAME, expectedDocument);

        GetResponse getResponse = elasticsearchServer
                .getClient()
                .prepareGet(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, expectedDocument.getTimestamp()),
                        ElasticsearchUtils.TYPE_NAME,
                        expectedDocument.getId())
                .setFields("_timestamp").execute().actionGet();
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", expectedDocument.getId(), getResponse.getId());
        assertEquals("Timestamp should match request timestamp", expectedDocument.getTimestamp(), getResponse.getField("_timestamp").getValue());
    }

    @Test
    public void testSaveSingleInvalidTable() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
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
                    getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);

        for (Document document : documents) {
            GetResponse getResponse = elasticsearchServer
                    .getClient()
                    .prepareGet(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()),
                            ElasticsearchUtils.TYPE_NAME,
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
                    getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
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
        JsonNode data = getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document document = new Document(id, System.currentTimeMillis(), data);
        document.setTimestamp(timestamp);
        dataStore.save(TestUtils.TEST_TABLE, document);

        Document responseDocument = queryStore.get(TestUtils.TEST_TABLE_NAME, id);
        assertNotNull(responseDocument);
        assertEquals(id, responseDocument.getId());
        assertEquals("Timestamp should match request timestamp", document.getTimestamp(), responseDocument.getTimestamp());
    }

    @Test
    public void testGetSingleInvalidId() throws Exception {
        try {
            queryStore.get(TestUtils.TEST_TABLE_NAME, UUID.randomUUID().toString());
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, qse.getErrorCode());
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
                            getMapper().valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
            idValues.get(id).setTimestamp(System.currentTimeMillis());
        }
        for (Document document : idValues.values()) {
            dataStore.save(TestUtils.TEST_TABLE, document);
        }

        List<Document> responseDocuments = queryStore.get(TestUtils.TEST_TABLE_NAME, ids);
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
            queryStore.get(TestUtils.TEST_TABLE_NAME, Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND, qse.getErrorCode());
        }
    }

    @Test
    public void testGetTableFieldMetadata() throws QueryStoreException, InterruptedException {
        queryStore.save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments());
        Thread.sleep(500);

        Set<FieldData> mappings = new HashSet<FieldData>();
        mappings.add(new FieldData("word", FieldType.STRING));
        mappings.add(new FieldData("data.data", FieldType.STRING));
        mappings.add(new FieldData("header.hello", FieldType.STRING));
        mappings.add(new FieldData("head.hello", FieldType.LONG));

        TableFieldMetadata tableFieldMetadata = new TableFieldMetadata(TestUtils.TEST_TABLE_NAME, mappings);
        TableFieldMetadata responseMapping = queryStore.fieldMetadata(TestUtils.TEST_TABLE_NAME);

        assertEquals(tableFieldMetadata.getTable(), responseMapping.getTable());
        assertTrue(tableFieldMetadata.getFieldData().equals(responseMapping.getFieldData()));
    }

    @Test
    public void testGetTableFieldMetadataNonExistingTable() throws QueryStoreException {
        try {
            queryStore.fieldMetadata(TestUtils.TEST_TABLE + "-test");
            fail();
        } catch (QueryStoreException qse) {
            assertEquals(QueryStoreException.ErrorCode.NO_SUCH_TABLE, qse.getErrorCode());
        }
    }

    @Test
    public void testGetTableFieldMetadataNoDocumentsInTable() throws QueryStoreException {
        TableFieldMetadata request = new TableFieldMetadata(TestUtils.TEST_TABLE_NAME, new HashSet<FieldData>());
        TableFieldMetadata response = queryStore.fieldMetadata(TestUtils.TEST_TABLE_NAME);

        assertEquals(request.getTable(), response.getTable());
        assertTrue(request.getFieldData().equals(response.getFieldData()));
    }
}
