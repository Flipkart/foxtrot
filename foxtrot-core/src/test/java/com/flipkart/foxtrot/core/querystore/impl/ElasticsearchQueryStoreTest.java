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
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.shash.hbase.ds.RowKeyDistributorByHashPrefix;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class ElasticsearchQueryStoreTest {
    private MockElasticsearchServer elasticsearchServer;
    private ElasticsearchQueryStore queryStore;
    private ObjectMapper mapper;
    private TableMetadataManager tableMetadataManager;
    private final DocumentTranslator translator = new DocumentTranslator(new RowKeyDistributorByHashPrefix(
            new RowKeyDistributorByHashPrefix.OneByteSimpleHash(32)));

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        DataStore dataStore = TestUtils.getDataStore();
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, mapper);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void testSaveSingle() throws Exception {
        Document originalDocument = new Document();
        originalDocument.setId(UUID.randomUUID().toString());
        originalDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        originalDocument.setData(data);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);
        final Document translatedDocument = translator.translate(tableMetadataManager.get(TestUtils.TEST_TABLE_NAME), originalDocument);
        GetResponse getResponse = elasticsearchServer
                .getClient()
                .prepareGet(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, originalDocument.getTimestamp()),
                        ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        translatedDocument.getId())
                .setFields("_timestamp").execute().actionGet();
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", translatedDocument.getId(), getResponse.getId());
        assertEquals("Timestamp should match request timestamp", translatedDocument.getTimestamp(), getResponse.getField("_timestamp").getValue());
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
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, qse.getCode());
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
        final List<Document> translatedDocuemtns = translator.translate(tableMetadataManager.get(TestUtils.TEST_TABLE_NAME), documents);
        for (Document document : translatedDocuemtns) {
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
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkEmptyList() throws Exception {
        List<Document> list = new Vector<Document>();
        try {
            queryStore.save(TestUtils.TEST_TABLE_NAME, list);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
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
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, qse.getCode());
        }
    }

    @Test
    public void testGetSingle() throws Exception {
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document document = new Document(id, System.currentTimeMillis(), data);
        document.setTimestamp(timestamp);
        queryStore.save(TestUtils.TEST_TABLE_NAME, document);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document responseDocument = queryStore.get(TestUtils.TEST_TABLE_NAME, id);
        assertNotNull(responseDocument);
        assertEquals(id, responseDocument.getId());
        assertEquals("Timestamp should match request timestamp", document.getTimestamp(), responseDocument.getTimestamp());
    }

    @Test
    public void testGetSingleInvalidId() throws Exception {
        try {
            queryStore.get(TestUtils.TEST_TABLE_NAME, UUID.randomUUID().toString());
            fail();
        } catch (FoxtrotException dse) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, dse.getCode());
        }
    }

    @Test
    public void testGetBulk() throws Exception {
        Map<String, Document> idValues = Maps.newHashMap();
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
        queryStore.save(TestUtils.TEST_TABLE_NAME, ImmutableList.copyOf(idValues.values()));
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        List<Document> responseDocuments = queryStore.getAll(TestUtils.TEST_TABLE_NAME, ids);
        HashMap<String, Document> responseIdValues = Maps.newHashMap();
        for (Document doc : responseDocuments) {
            responseIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", responseDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", responseIdValues.containsKey(id));
            assertNotNull(responseIdValues.get(id));
            assertEquals(id, responseIdValues.get(id).getId());
            assertEquals("Timestamp should match request timestamp", idValues.get(id).getTimestamp(), responseIdValues.get(id).getTimestamp());
            System.out.println("OK: " + id);
        }
    }

    @Test
    public void testGetBulkInvalidIds() throws Exception {
        try {
            queryStore.getAll(TestUtils.TEST_TABLE_NAME, Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, e.getCode());
        }
    }

    @Test
    public void testGetFieldMappings() throws FoxtrotException, InterruptedException {
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
    public void testGetFieldMappingsNonExistingTable() throws FoxtrotException {
        try {
            queryStore.getFieldMappings(TestUtils.TEST_TABLE + "-test");
            fail();
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.TABLE_NOT_FOUND, qse.getCode());
        }
    }

    @Test
    public void testGetFieldMappingsNoDocumentsInTable() throws FoxtrotException {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<>());
        TableFieldMapping response = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);

        assertEquals(request.getTable(), response.getTable());
        assertTrue(request.getMappings().equals(response.getMappings()));
    }

    @Test
    public void testEsClusterHealth() throws ExecutionException, InterruptedException, FoxtrotException {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        ClusterHealthResponse clusterHealth = queryStore.getClusterHealth();
        assertEquals("elasticsearch",clusterHealth.getClusterName());
        assertEquals(1,clusterHealth.getIndices().size());
    }

    @Test
    public void testEsNodesStats() throws FoxtrotException, ExecutionException, InterruptedException {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        NodesStatsResponse clusterHealth = queryStore.getNodeStats();
        assertNotNull(clusterHealth);
        assertEquals(1, clusterHealth.getNodesMap().size());
    }

    @Test
    public void testIndicesStats() throws FoxtrotException, ExecutionException, InterruptedException {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchServer.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        IndicesStatsResponse clusterHealth = queryStore.getIndicesStats();
        assertEquals(10,clusterHealth.getPrimaries().getDocs().getCount());
        assertNotEquals(0,clusterHealth.getTotal().getStore().getSizeInBytes());
        assertNotEquals(0,clusterHealth.getPrimaries().getStore().getSizeInBytes());

    }
}
