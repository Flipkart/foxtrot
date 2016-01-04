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
package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.shash.hbase.ds.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 15/04/14.
 */

public class HBaseDataStoreTest {
    private HBaseDataStore HBaseDataStore;
    private HTableInterface tableInterface;
    private HbaseTableConnection hBaseTableConnection;
    private ObjectMapper mapper = new ObjectMapper();

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DATA_FIELD_NAME = Bytes.toBytes("data");
    private static final String TEST_APP_NAME = "test-app";
    private static final Table TEST_APP = new Table(TEST_APP_NAME, 7);
    private final DocumentTranslator translator = new DocumentTranslator(new RowKeyDistributorByHashPrefix(
            new RowKeyDistributorByHashPrefix.OneByteSimpleHash(32)));


    @Before
    public void setUp() throws Exception {
        tableInterface = MockHTable.create();
        tableInterface = spy(tableInterface);
        hBaseTableConnection = Mockito.mock(HbaseTableConnection.class);
        when(hBaseTableConnection.getTable(Matchers.<Table>any())).thenReturn(tableInterface);
        when(hBaseTableConnection.getHbaseConfig()).thenReturn(new HbaseConfig());
        HBaseDataStore = new HBaseDataStore(hBaseTableConnection, mapper);
    }

    @Test
    public void testSaveSingle() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        HBaseDataStore.save(TEST_APP, expectedDocument);
        validateSave(expectedDocument);
    }

    @Test
    public void testSaveSingleNullDocument() throws Exception {
        Document document = null;
        try {
            HBaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleNullId() throws Exception {
        Document document = new Document(null, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
        try {
            HBaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleNullData() throws Exception {
        Document document = new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null);
        try {
            HBaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleHBaseWriteException() throws Exception {
        Document document = new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
        doThrow(new IOException())
                .when(tableInterface)
                .put(Matchers.<Put>any());
        try {
            HBaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleHBaseCloseException() throws Exception {
        Document document = new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        HBaseDataStore.save(TEST_APP, document);
        verify(tableInterface, times(1)).close();
    }

    @Test
    public void testSaveBulk() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            final String id = UUID.randomUUID().toString();
            documents.add(new Document(
                    id,
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        HBaseDataStore.saveAll(TEST_APP, documents);
        for (Document document : documents) {
            validateSave(document);
        }
    }

    @Test
    public void testSaveBulkNullDocuments() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(null);
        }
        try {
            HBaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkNullIdList() throws Exception {
        List<Document> documents = null;
        try {
            HBaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(null, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        try {
            HBaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
        }
        try {
            HBaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkHBaseWriteException() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST", "TEST"))));
        doThrow(new IOException())
                .when(tableInterface)
                .put(Matchers.anyListOf(Put.class));
        try {
            HBaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkHBaseCloseException() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_SAVE_TEST"))));
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        HBaseDataStore.saveAll(TEST_APP, documents);
        verify(tableInterface, times(1)).close();
    }

    public void validateSave(Document savedDocument) throws Exception {
        String rowkey = translator.rawStorageIdFromDocument(TEST_APP, savedDocument);
        Get get = new Get(Bytes.toBytes(translator.generateScalableKey(translator.rawStorageIdFromDocumentId(TEST_APP, rowkey))));
        Result result = tableInterface.get(get);
        assertNotNull("Get for Id should not be null", result);
        Document actualDocument = new Document(savedDocument.getId(),
                savedDocument.getTimestamp(),
                mapper.readTree(result.getValue(COLUMN_FAMILY, DATA_FIELD_NAME)));
        compare(savedDocument, actualDocument);
    }

    private String v1FormatKey(String id) {
        return String.format("%s:%s", id, TEST_APP_NAME);
    }

    @Test
    public void testGetSingleV1Format() throws Exception {
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        final String newId = v1FormatKey(id);
        Document expectedDocument = new Document(newId, System.currentTimeMillis(),
                new DocumentMetadata(newId, "row.1"), data);
        tableInterface.put(HBaseDataStore.getPutForDocument(expectedDocument));
        Document actualDocument = HBaseDataStore.get(TEST_APP, id);
        compare(expectedDocument, actualDocument);
    }

    @Test
    public void testGetSingleV2Format() throws Exception {
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document originalDocument = new Document(id, timestamp, data);
        final String newId = translator.generateScalableKey(translator.rawStorageIdFromDocument(TEST_APP, originalDocument));
        tableInterface.put(HBaseDataStore.getPutForDocument(translator.translate(TEST_APP, originalDocument)));
        Document actualDocument = HBaseDataStore.get(TEST_APP, newId);
        compare(originalDocument, actualDocument);
    }

    @Test
    public void testGetSingleMissingDocument() {
        try {
            HBaseDataStore.get(TEST_APP, UUID.randomUUID().toString());
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, ex.getCode());
        }
    }

    @Test
    public void testGetSingleHBaseReadException() throws Exception {
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        tableInterface.put(HBaseDataStore.getPutForDocument(expectedDocument));
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.<Get>any());
        try {
            HBaseDataStore.get(TEST_APP, id);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGetSingleHBaseCloseException() throws Exception {
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        Document translated = translator.translate(TEST_APP, expectedDocument);
        tableInterface.put(HBaseDataStore.getPutForDocument(translated));
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        HBaseDataStore.get(TEST_APP, translated.getId());
        verify(tableInterface, times(1)).close();
    }

    @Test
    public void testV1GetBulk() throws Exception {
        Map<String, Document> idValues = Maps.newHashMap();
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        HashMap<String, Document> actualIdValues = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"));
            String rawId = v1FormatKey(id);
            ids.add(id);
            Document document = new Document(rawId, timestamp,
                    new DocumentMetadata(id, rawId), data);
            putList.add(HBaseDataStore.getPutForDocument(document));
            idValues.put(id, new Document(id, timestamp, data));
        }
        tableInterface.put(putList);
        List<Document> actualDocuments = HBaseDataStore.getAll(TEST_APP, ids);
        for (Document doc : actualDocuments) {
            actualIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", actualDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", actualIdValues.containsKey(id));
            compare(idValues.get(id), actualIdValues.get(id));
        }
    }

    @Test
    public void testV2GetBulk() throws Exception {
        Map<String, Document> idValues = Maps.newHashMap();
        List<String> ids = new Vector<String>();
        List<String> rawIds = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        HashMap<String, Document> actualIdValues = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"));
            ids.add(id);
            Document document = new Document(id, timestamp, data);
            Document translated = translator.translate(TEST_APP, document);
            putList.add(HBaseDataStore.getPutForDocument(translated));
            idValues.put(id, new Document(id, timestamp, data));
            rawIds.add(translated.getId());
        }
        tableInterface.put(putList);
        List<Document> actualDocuments = HBaseDataStore.getAll(TEST_APP, rawIds);
        for (Document doc : actualDocuments) {
            actualIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", actualDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", actualIdValues.containsKey(id));
            compare(idValues.get(id), actualIdValues.get(id));
        }
    }

    @Test
    public void testGetBulkNullIdList() throws Exception {
        try {
            HBaseDataStore.getAll(TEST_APP, null);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testGetBulkMissingDocument() throws Exception {
        List<String> ids = new Vector<String>();
        for (int i = 0; i < 10; i++) {
            ids.add(UUID.randomUUID().toString());
        }
        try {
            HBaseDataStore.getAll(TEST_APP, ids);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, ex.getCode());
        }
    }

    @Test
    public void testGetBulkHBaseReadException() throws Exception {
        List<String> ids = new ArrayList<>();
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(),
                    new DocumentMetadata(id, String.format("row:%d", i)),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST")));
            putList.add(HBaseDataStore.getPutForDocument(document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.anyListOf(Get.class));
        try {
            HBaseDataStore.getAll(TEST_APP, ids);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGetBulkHBaseCloseException() throws Exception {
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(),
                    new DocumentMetadata(id, String.format("row:%d", i)),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST")));
            putList.add(HBaseDataStore.getPutForDocument(document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        HBaseDataStore.getAll(TEST_APP, ids);
        verify(tableInterface, times(1)).close();
    }

    @Test
    public void testBasicSetGet() throws Exception {
        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document originalDocument = new Document(id, timestamp, data);
        final String newId = translator.generateScalableKey(translator.rawStorageIdFromDocument(TEST_APP, originalDocument));
        HBaseDataStore.save(TEST_APP, originalDocument);
        Document actualDocument = HBaseDataStore.get(TEST_APP, newId);
        compare(originalDocument, actualDocument);
    }

    @Test
    public void testBasicBulkGetSet() throws Exception {
        Map<String, Document> idValues = Maps.newHashMap();
        List<String> ids = new Vector<String>();
        HashMap<String, Document> actualIdValues = Maps.newHashMap();
        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"));
            ids.add(id);
            Document document = new Document(id, timestamp, data);
            idValues.put(id, document);
            documents.add(document);
        }
        List<Document> translatedDocs = HBaseDataStore.saveAll(TEST_APP, documents);
        List<String> rawIds = translatedDocs.stream().map(Document::getId).collect(Collectors.toCollection(ArrayList::new));
        List<Document> actualDocuments = HBaseDataStore.getAll(TEST_APP, rawIds);
        for (Document doc : actualDocuments) {
            actualIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", actualDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", actualIdValues.containsKey(id));
            compare(idValues.get(id), actualIdValues.get(id));
        }
    }

    public void compare(Document expected, Document actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
        String expectedData = mapper.writeValueAsString(expected.getData());
        String actualData = mapper.writeValueAsString(actual.getData());
        assertEquals("Actual data should match expected data", expectedData, actualData);
    }
}
