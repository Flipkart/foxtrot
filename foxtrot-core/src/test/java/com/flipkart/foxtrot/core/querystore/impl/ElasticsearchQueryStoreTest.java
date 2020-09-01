/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.dropwizard.jackson.Jackson;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.val;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class ElasticsearchQueryStoreTest {

    private static final ObjectMapper mapper = Jackson.newObjectMapper();

    private static ElasticsearchConnection elasticsearchConnection;
    private static HazelcastInstance hazelcastInstance;

    private DataStore dataStore;
    private ElasticsearchQueryStore queryStore;
    private TableMetadataManager tableMetadataManager;
    private TextNodeRemoverConfiguration removerConfiguration;

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        ElasticsearchUtils.initializeMappings(elasticsearchConnection.getClient());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        elasticsearchConnection.stop();
    }

    @Before
    public void setUp() throws Exception {
        this.dataStore = Mockito.mock(DataStore.class);

        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());

        CardinalityConfig cardinalityConfig = new CardinalityConfig("true",
                String.valueOf(ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE));
        TestUtils.ensureIndex(elasticsearchConnection, TableMapStore.TABLE_META_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, DistributedTableMetadataManager.CARDINALITY_CACHE_INDEX);
        this.tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection,
                mapper, cardinalityConfig);
        tableMetadataManager.start();
        tableMetadataManager.save(Table.builder()
                .name(TestUtils.TEST_TABLE_NAME)
                .ttl(30)
                .build());
        this.removerConfiguration = spy(TextNodeRemoverConfiguration.builder()
                .build());
        List<IndexerEventMutator> mutators = Lists.newArrayList(new LargeTextNodeRemover(mapper, removerConfiguration));
        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore,
                mutators, mapper, cardinalityConfig);
    }

    @After
    public void tearDown() throws Exception {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }


    @Test
    public void testSaveSingleRawKeyVersion1() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);

        GetResponse getResponse = elasticsearchConnection.getClient()
                .get(new GetRequest(
                        ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, originalDocument.getTimestamp()),
                        ElasticsearchUtils.DOCUMENT_TYPE_NAME, originalDocument.getId()).storedFields(
                        ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME), RequestOptions.DEFAULT);
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", originalDocument.getId(), getResponse.getId());
    }

    private Document createDummyDocument() {
        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        document.setData(data);
        return document;
    }

    @Test
    public void testSaveSingleRawKeyVersion2() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion2(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);

        GetResponse getResponse = elasticsearchConnection.getClient()
                .get(new GetRequest(
                        ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, originalDocument.getTimestamp()),
                        ElasticsearchUtils.DOCUMENT_TYPE_NAME, translatedDocument.getId()).storedFields(
                        ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME), RequestOptions.DEFAULT);
        assertTrue("Id should exist in ES", getResponse.isExists());
        assertEquals("Id should match requestId", translatedDocument.getId(), getResponse.getId());
    }

    @Test
    public void testSaveBulkLargeTextNodeWithBlockingEnabled() throws Exception {
        when(removerConfiguration.getBlockPercentage()).thenReturn(100);
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createLargeDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);
        val currentIndex = ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME,
                originalDocument.getTimestamp());
        val response = elasticsearchConnection.getClient()
                .get(new GetRequest(currentIndex, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        originalDocument.getId()).storedFields(ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME,
                        "testField", "testLargeField"), RequestOptions.DEFAULT);
        assertTrue(response.isExists());
        val request = new GetFieldMappingsRequest();
        request.indices(currentIndex);
        request.fields("*");

        val mappings = elasticsearchConnection.getClient()
                .indices()
                .getFieldMapping(request, RequestOptions.DEFAULT);

        Set<String> expectedFields = Sets.newHashSet("_index", "date.minuteOfHour", "date.year", "date.dayOfMonth",
                "testField", "testField.analyzed", "_all", "date.dayOfWeek", "date.minuteOfDay", "_parent",
                "date.monthOfYear", "__FOXTROT_METADATA__.time", "time.date", "_version", "date.weekOfYear", "_routing",
                "__FOXTROT_METADATA__.rawStorageId", "_type", "__FOXTROT_METADATA__.id", "date.hourOfDay", "_seq_no",
                "_field_names", "_source", "_id", "time", "_uid", "_ignored", "eventData.funnelInfo.funnelId");
        assertTrue(ObjectUtils.equals(expectedFields, mappings.mappings()
                .get(currentIndex)
                .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .keySet()));
    }

    @Test
    public void testSaveBulkLargeTextNodeWithBlockingDisabled() throws Exception {
        when(removerConfiguration.getBlockPercentage()).thenReturn(0);

        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createLargeDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);
        val currentIndex = ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME,
                originalDocument.getTimestamp());
        val response = elasticsearchConnection.getClient()
                .get(new GetRequest(currentIndex, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        originalDocument.getId()).storedFields(ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME,
                        "testField", "testLargeField"), RequestOptions.DEFAULT);
        assertTrue(response.isExists());
        val request = new GetFieldMappingsRequest();
        request.indices(currentIndex);
        request.fields("*");

        val mappings = elasticsearchConnection.getClient()
                .indices()
                .getFieldMapping(request, RequestOptions.DEFAULT);

        Set<String> expectedFields = Sets.newHashSet("_index", "date.minuteOfHour", "date.year", "date.dayOfMonth",
                "testField", "testField.analyzed", "testLargeField", "testLargeField.analyzed", "_all",
                "date.dayOfWeek", "date.minuteOfDay", "_parent", "date.monthOfYear", "__FOXTROT_METADATA__.time",
                "time.date", "_version", "date.weekOfYear", "_routing", "__FOXTROT_METADATA__.rawStorageId", "_type",
                "__FOXTROT_METADATA__.id", "date.hourOfDay", "_seq_no", "_field_names", "_source", "_id", "time",
                "_uid", "_ignored", "eventData.funnelInfo.funnelId");
        final Set<String> received = mappings.mappings()
                .get(currentIndex)
                .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .keySet();
        assertEquals(expectedFields, received);
    }

    @Test
    public void testSaveBulkNestedLargeTextNode() throws Exception {
        when(removerConfiguration.getBlockPercentage()).thenReturn(100);
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createLargeNestedDocumentWithLargeTextNodeAsKey();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);

        val currentIndex = ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME,
                originalDocument.getTimestamp());

        String[] fields = {ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME, "testField",
                String.format("testLargeField%s", StringUtils.repeat(".testField", 5))};
        val response = elasticsearchConnection.getClient()
                .get(new GetRequest(currentIndex, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        originalDocument.getId()).storedFields(fields), RequestOptions.DEFAULT);
        assertTrue(response.isExists());

        val request = new GetFieldMappingsRequest();
        request.indices(currentIndex);
        request.fields("*");

        val mappings = elasticsearchConnection.getClient()
                .indices()
                .getFieldMapping(request, RequestOptions.DEFAULT);

        Set<String> expectedFields = Sets.newHashSet("_index", "date.minuteOfHour", "date.year", "date.dayOfMonth",
                "testField", "testField.analyzed", "_all", "date.dayOfWeek", "date.minuteOfDay", "_parent",
                "date.monthOfYear", "__FOXTROT_METADATA__.time", "time.date", "_version", "date.weekOfYear", "_routing",
                "__FOXTROT_METADATA__.rawStorageId", "_type", "__FOXTROT_METADATA__.id", "date.hourOfDay", "_seq_no",
                "_field_names", "_source", "_id", "time", "_uid", "_ignored", "eventData.funnelInfo.funnelId");
        final Set<String> received = mappings.mappings()
                .get(currentIndex)
                .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .keySet();
        assertEquals(expectedFields, received);
    }

    @Test
    public void testSaveBulkNestedArrayLargeTextNode() throws Exception {
        when(removerConfiguration.getBlockPercentage()).thenReturn(100);
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document originalDocument = createLargeNestedDocumentWithLargeTextNodeAsArrayValue();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, originalDocument);
        doReturn(translatedDocument).when(dataStore)
                .save(table, originalDocument);
        queryStore.save(TestUtils.TEST_TABLE_NAME, originalDocument);

        val currentIndex = ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME,
                originalDocument.getTimestamp());

        String[] fields = {ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME, "testField",
                String.format("testLargeField%s", StringUtils.repeat(".testField", 5))};
        val response = elasticsearchConnection.getClient()
                .get(new GetRequest(currentIndex, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        originalDocument.getId()).storedFields(fields), RequestOptions.DEFAULT);
        assertTrue(response.isExists());

        val request = new GetFieldMappingsRequest();
        request.indices(currentIndex);
        request.fields("*");

        val mappings = elasticsearchConnection.getClient()
                .indices()
                .getFieldMapping(request, RequestOptions.DEFAULT);
        val expectedFields = Sets.newHashSet("_index", "date.minuteOfHour", "date.year", "date.dayOfMonth", "testField",
                "testField.analyzed", "_all", "date.dayOfWeek", "date.minuteOfDay", "_parent", "date.monthOfYear",
                "__FOXTROT_METADATA__.time", "time.date", "_version", "date.weekOfYear", "_routing",
                "__FOXTROT_METADATA__.rawStorageId", "_type", "__FOXTROT_METADATA__.id", "date.hourOfDay", "_seq_no",
                "_field_names", "_source", "_id", "time", "_uid", "_ignored",
                "testLargeField.testField.testField.testField.testField_array",
                "testLargeField.testField.testField.testField.testField_array.analyzed",
                "eventData.funnelInfo.funnelId");
        assertTrue(ObjectUtils.equals(expectedFields, mappings.mappings()
                .get(currentIndex)
                .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .keySet()));
    }

    @Test
    public void testSaveSingleInvalidTable() throws Exception {
        Document expectedDocument = createDummyDocument();
        try {
            queryStore.save(TestUtils.TEST_TABLE + "-missing", expectedDocument);
            fail();
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.INVALID_REQUEST, qse.getCode());
        }
    }

    @Test
    public void testSaveBulkRawKeyVersion1() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }

        List<Document> translatedDocuments = Lists.newArrayList();
        translatedDocuments.addAll(documents.stream()
                .map(document -> TestUtils.translatedDocumentWithRowKeyVersion1(table, document))
                .collect(Collectors.toList()));

        doReturn(translatedDocuments).when(dataStore)
                .saveAll(table, documents);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);

        for (Document document : documents) {
            GetResponse getResponse = elasticsearchConnection.getClient()
                    .get(new GetRequest(
                            ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()),
                            ElasticsearchUtils.DOCUMENT_TYPE_NAME, document.getId()).storedFields(
                            ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME), RequestOptions.DEFAULT);
            assertTrue("Id should exist in ES", getResponse.isExists());
            assertEquals("Id should match requestId", document.getId(), getResponse.getId());
        }
    }

    @Test
    public void testSaveBulkRawKeyVersion2() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }

        List<Document> translatedDocuments = Lists.newArrayList();
        translatedDocuments.addAll(documents.stream()
                .map(document -> TestUtils.translatedDocumentWithRowKeyVersion2(table, document))
                .collect(Collectors.toList()));

        doReturn(translatedDocuments).when(dataStore)
                .saveAll(table, documents);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);

        for (Document document : documents) {
            GetResponse getResponse = elasticsearchConnection.getClient()
                    .get(new GetRequest(
                            ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()),
                            ElasticsearchUtils.DOCUMENT_TYPE_NAME, document.getId()).storedFields(
                            ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME), RequestOptions.DEFAULT);
            assertFalse("Id should not exist in ES", getResponse.isExists());
        }

        for (Document document : translatedDocuments) {
            GetResponse getResponse = elasticsearchConnection.getClient()
                    .get(new GetRequest(
                            ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE_NAME, document.getTimestamp()),
                            ElasticsearchUtils.DOCUMENT_TYPE_NAME, document.getId()).storedFields(
                            ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME), RequestOptions.DEFAULT);

            assertTrue("Id should exist in ES", getResponse.isExists());
            assertEquals("Id should match requestId", document.getId(), getResponse.getId());
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
            documents.add(createDummyDocument());
        }
        try {
            queryStore.save(TestUtils.TEST_TABLE + "-missing", documents);
            fail();
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.INVALID_REQUEST, qse.getCode());
        }
    }

    @Test
    public void testGetSingleRawKeyVersion1() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document document = createDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, document);

        doReturn(translatedDocument).when(dataStore)
                .save(table, document);
        doReturn(translatedDocument).when(dataStore)
                .get(table, document.getId());

        queryStore.save(TestUtils.TEST_TABLE_NAME, document);

        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document responseDocument = queryStore.get(TestUtils.TEST_TABLE_NAME, document.getId());
        assertNotNull(responseDocument);
        assertEquals(document.getId(), responseDocument.getId());
    }

    @Test
    public void testGetSingleRawKeyVersion2() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Document document = createDummyDocument();
        Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion2(table, document);

        doReturn(translatedDocument).when(dataStore)
                .save(table, document);
        doReturn(document).when(dataStore)
                .get(table, translatedDocument.getId());

        queryStore.save(TestUtils.TEST_TABLE_NAME, document);

        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        Document responseDocument = queryStore.get(TestUtils.TEST_TABLE_NAME, document.getId());
        assertNotNull(responseDocument);
        assertEquals(document.getId(), responseDocument.getId());
    }

    @Test
    public void testGetSingleInvalidId() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);
        doThrow(FoxtrotExceptions.createMissingDocumentException(table, UUID.randomUUID()
                .toString())).when(dataStore)
                .get(any(Table.class), anyString());
        try {
            queryStore.get(TestUtils.TEST_TABLE_NAME, UUID.randomUUID()
                    .toString());
            fail();
        } catch (FoxtrotException dse) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, dse.getCode());
        }
    }

    @Test
    public void testGetBulkRawKeyVersion1() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Map<String, Document> idValues = Maps.newLinkedHashMap();
        Map<String, Document> translatedIdValues = Maps.newLinkedHashMap();

        List<String> ids = new Vector<String>();
        for (int i = 0; i < 10; i++) {
            Document document = createDummyDocument();
            Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion1(table, document);

            ids.add(document.getId());
            idValues.put(document.getId(), document);
            translatedIdValues.put(document.getId(), translatedDocument);
        }

        doReturn(ImmutableList.copyOf(translatedIdValues.values())).when(dataStore)
                .saveAll(table, ImmutableList.copyOf(idValues.values()));
        doReturn(ImmutableList.copyOf(idValues.values())).when(dataStore)
                .getAll(table, ids);

        queryStore.save(TestUtils.TEST_TABLE_NAME, ImmutableList.copyOf(idValues.values()));
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));

        List<Document> responseDocuments = queryStore.getAll(TestUtils.TEST_TABLE_NAME, ids);
        HashMap<String, Document> responseIdValues = Maps.newHashMap();
        for (Document doc : responseDocuments) {
            responseIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", responseDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", responseIdValues.containsKey(id));
            assertNotNull(responseIdValues.get(id));
            assertEquals(id, responseIdValues.get(id)
                    .getId());
        }
    }

    @Test
    public void testGetBulkRawKeyVersion2() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);

        Map<String, Document> idValues = Maps.newLinkedHashMap();
        Map<String, Document> translatedIdValues = Maps.newLinkedHashMap();

        List<String> ids = Lists.newArrayList();
        List<String> translatedIds = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            Document document = createDummyDocument();
            Document translatedDocument = TestUtils.translatedDocumentWithRowKeyVersion2(table, document);

            ids.add(document.getId());
            translatedIds.add(translatedDocument.getId());
            idValues.put(document.getId(), document);
            translatedIdValues.put(document.getId(), translatedDocument);
        }

        doReturn(ImmutableList.copyOf(translatedIdValues.values())).when(dataStore)
                .saveAll(table, ImmutableList.copyOf(idValues.values()));
        doReturn(ImmutableList.copyOf(idValues.values())).when(dataStore)
                .getAll(table, translatedIds);

        queryStore.save(TestUtils.TEST_TABLE_NAME, ImmutableList.copyOf(idValues.values()));
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));

        List<Document> responseDocuments = queryStore.getAll(TestUtils.TEST_TABLE_NAME, ids);
        HashMap<String, Document> responseIdValues = Maps.newHashMap();
        for (Document doc : responseDocuments) {
            responseIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", responseDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", responseIdValues.containsKey(id));
            assertNotNull(responseIdValues.get(id));
            assertEquals(id, responseIdValues.get(id)
                    .getId());
        }
    }

    @Test
    public void testGetBulkInvalidIds() throws Exception {
        Table table = tableMetadataManager.get(TestUtils.TEST_TABLE_NAME);
        doThrow(FoxtrotExceptions.createMissingDocumentException(table, UUID.randomUUID()
                .toString())).when(dataStore)
                .getAll(any(Table.class), anyListOf(String.class));
        try {
            queryStore.getAll(TestUtils.TEST_TABLE_NAME, Arrays.asList(UUID.randomUUID()
                    .toString(), UUID.randomUUID()
                    .toString()));
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, e.getCode());
        }
    }

    /*@Test
    @Ignore
    public void testGetFieldMappings() throws FoxtrotException, InterruptedException {
        doReturn(TestUtils.getMappingDocuments(mapper)).when(dataStore)
                .saveAll(any(Table.class), anyListOf(Document.class));
        queryStore.save(TestUtils.TEST_TABLE_NAME, TestUtils.getMappingDocuments(mapper));
        await().pollDelay(500, TimeUnit.MILLISECONDS)
                .until(() -> true);

        Set<FieldMetadata> mappings = new HashSet<FieldMetadata>();
        mappings.add(FieldMetadata.builder()
                .field("time")
                .type(FieldType.LONG)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("word")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("data.data")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("header.hello")
                .type(FieldType.STRING)
                .build());
        mappings.add(FieldMetadata.builder()
                .field("head.hello")
                .type(FieldType.LONG)
                .build());

        TableFieldMapping tableFieldMapping = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, mappings);
        TableFieldMapping responseMapping = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);

        assertEquals(tableFieldMapping.getTable(), responseMapping.getTable());
        assertEquals(tableFieldMapping.getMappings(), responseMapping.getMappings());
    }*/

    @Test
    public void testGetFieldMappingsNonExistingTable() throws FoxtrotException {
        try {
            queryStore.getFieldMappings(TestUtils.TEST_TABLE + "-test");
            fail();
        } catch (FoxtrotException qse) {
            assertEquals(ErrorCode.INVALID_REQUEST, qse.getCode());
        }
    }

    @Test
    public void testGetFieldMappingsNoDocumentsInTable() throws FoxtrotException {
        TableFieldMapping request = new TableFieldMapping(TestUtils.TEST_TABLE_NAME, new HashSet<>());
        TableFieldMapping response = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);

        assertEquals(request.getTable(), response.getTable());
        assertTrue(request.getMappings()
                .equals(response.getMappings()));
    }

    @Test
    public void testEsClusterHealth() throws ExecutionException, InterruptedException, FoxtrotException {
        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }
        doReturn(documents).when(dataStore)
                .saveAll(any(Table.class), anyListOf(Document.class));
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        ClusterHealthResponse clusterHealth = queryStore.getClusterHealth();
        assertEquals("elasticsearch", clusterHealth.getClusterName());
        assertTrue(clusterHealth.getIndices()
                .size() > 0);
    }

    @Test
    public void testEsNodesStats() throws FoxtrotException, ExecutionException, InterruptedException {
        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }
        doReturn(documents).when(dataStore)
                .saveAll(any(Table.class), anyListOf(Document.class));

        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        JsonNode clusterHealth = queryStore.getNodeStats();
        assertNotNull(clusterHealth);
        assertEquals(1, clusterHealth.get("nodes")
                .size());
    }

    @Test
    public void testIndicesStats() throws FoxtrotException, ExecutionException, InterruptedException {
        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }
        doReturn(documents).when(dataStore)
                .saveAll(any(Table.class), anyListOf(Document.class));

        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));
        JsonNode clusterHealth = queryStore.getIndicesStats();

        assertEquals(10, clusterHealth.at("/_all/primaries/docs/count")
                .asInt());
        assertEquals(10, clusterHealth.at("/_all/primaries/docs/count")
                .asInt());

        assertNotEquals(0, clusterHealth.at("/_all/total/store/size_in_bytes")
                .asLong());
        assertNotEquals(0, clusterHealth.at("/_all/primaries/store/size_in_bytes")
                .asLong());

    }

    /*@Test
    @Ignore
    public void testEstimation() throws Exception {
        doReturn(TestUtils.getFieldCardinalityEstimationDocuments(mapper)).when(dataStore)
                .saveAll(any(Table.class), anyListOf(Document.class));
        queryStore.save(TestUtils.TEST_TABLE.getName(), TestUtils.getFieldCardinalityEstimationDocuments(mapper));
        elasticsearchConnection.refresh(ElasticsearchUtils.getIndices(TestUtils.TEST_TABLE_NAME));

        TableFieldMapping mappings = queryStore.getFieldMappings(TestUtils.TEST_TABLE_NAME);
        Assert.assertNotNull(mappings);
        Assert.assertTrue(mappings.getMappings()
                .stream()
                .filter(fieldMetadata -> fieldMetadata.getType()
                        .equals(FieldType.BOOLEAN))
                .filter(fieldMetadata -> fieldMetadata.getEstimationData() != null && fieldMetadata.getEstimationData()
                        .getType()
                        .equals(EstimationDataType.FIXED))
                .count() == 1);
        Assert.assertTrue(mappings.getMappings()
                .stream()
                .filter(fieldMetadata -> fieldMetadata.getType()
                        .equals(FieldType.LONG))
                .filter(fieldMetadata -> fieldMetadata.getEstimationData() != null && fieldMetadata.getEstimationData()
                        .getType()
                        .equals(EstimationDataType.PERCENTILE))
                .count() == 2);
        long numStringFields = mappings.getMappings()
                .stream()
                .filter(fieldMetadata -> fieldMetadata.getType()
                        .equals(FieldType.STRING))
                .count();
        Assert.assertTrue(mappings.getMappings()
                .stream()
                .filter(fieldMetadata -> fieldMetadata.getType()
                        .equals(FieldType.STRING))
                .filter(fieldMetadata -> fieldMetadata.getEstimationData() != null && fieldMetadata.getEstimationData()
                        .getType() == EstimationDataType.CARDINALITY)
                .count() == numStringFields);
    }*/

    private Document createLargeDummyDocument() {
        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        Map<String, Object> data = new HashMap<>();
        data.put("testField", "SINGLE_SAVE");
        data.put("testLargeField", StringUtils.repeat("*", 5000));
        document.setData(mapper.valueToTree(data));
        return document;
    }

    private Document createLargeNestedDocumentWithLargeTextNodeAsKey() {
        return Document.builder()
                .id(UUID.randomUUID()
                        .toString())
                .timestamp(System.currentTimeMillis())
                .data(mapper.createObjectNode()
                        .put("testField", "SINGLE_SAVE")
                        .set("testLargeField", createNestedObject("testField", StringUtils.repeat("*", 5000))))
                .build();
    }

    private Document createLargeNestedDocumentWithLargeTextNodeAsArrayValue() {
        return Document.builder()
                .id(UUID.randomUUID()
                        .toString())
                .timestamp(System.currentTimeMillis())
                .data(mapper.createObjectNode()
                        .put("testField", "SINGLE_SAVE")
                        .set("testLargeField", createNestedArray("testField", StringUtils.repeat("*", 5000))))
                .build();
    }


    private ObjectNode createNestedObject(String field,
                                          String value) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.with(field)
                .with(field)
                .with(field)
                .put(field, value);
        return objectNode;
    }

    private ObjectNode createNestedArray(String field,
                                         String value) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.with(field)
                .with(field)
                .with(field)
                .put(field, value)
                .put(field + "_array", UUID.randomUUID()
                        .toString());
        System.out.println(objectNode);
        return objectNode;
    }
}
