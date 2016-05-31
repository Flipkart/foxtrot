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
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 15/04/14.
 */

public class HBaseDataStoreTest {
    private HBaseDataStore hbaseDataStore;
    private org.apache.hadoop.hbase.client.Table tableInterface;
    private HbaseTableConnection hbaseTableConnection;
    private ObjectMapper mapper = new ObjectMapper();

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DATA_FIELD_NAME = Bytes.toBytes("data");
    private static final String TEST_APP_NAME = "test-app";
    private static final Table TEST_APP = new Table(TEST_APP_NAME, 7);

    @Before
    public void setUp() throws Exception {
        tableInterface = MockHTable.create();
        tableInterface = spy(tableInterface);
        this.hbaseTableConnection = Mockito.mock(HbaseTableConnection.class);
        when(hbaseTableConnection.getTable(Matchers.<Table>any())).thenReturn(tableInterface);
        when(hbaseTableConnection.getHbaseConfig()).thenReturn(new HbaseConfig());
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection, mapper,
                new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1()));
    }

    @Test
    public void testSaveSingle() throws Exception {
        // rawKeyVersion 1.0
        DocumentTranslator documentTranslator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1());
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection, mapper, documentTranslator);
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        hbaseDataStore.save(TEST_APP, expectedDocument);
        validateSave(v1FormatKey(expectedDocument.getId()), expectedDocument);

        // rawKeyVersion 2.0
        documentTranslator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV2());
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection, mapper, documentTranslator);
        expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        hbaseDataStore.save(TEST_APP, expectedDocument);
        validateSave(documentTranslator.translate(TEST_APP, expectedDocument).getMetadata().getRawStorageId(), expectedDocument);
    }

    @Test
    public void testSaveSingleNullDocument() throws Exception {
        Document document = null;
        try {
            hbaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleNullId() throws Exception {
        Document document = createDummyDocument();
        document.setId(null);
        try {
            hbaseDataStore.save(TEST_APP, document);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveSingleNullData() throws Exception {
        Document document = new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null);
        try {
            hbaseDataStore.save(TEST_APP, document);
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
            hbaseDataStore.save(TEST_APP, document);
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
        hbaseDataStore.save(TEST_APP, document);
        verify(tableInterface, times(1)).close();
    }

    @Test
    public void testSaveBulk() throws Exception {
        DocumentTranslator documentTranslator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1());
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection, mapper, documentTranslator);

        List<Document> documents = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            documents.add(createDummyDocument());
        }
        hbaseDataStore.saveAll(TEST_APP, documents);
        for (Document document : documents) {
            validateSave(v1FormatKey(document.getId()), document);
        }
    }

    @Test
    public void testSaveBulkNullDocuments() throws Exception {
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(null);
        }
        try {
            hbaseDataStore.saveAll(TEST_APP, documents);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        }
    }

    @Test
    public void testSaveBulkNullIdList() throws Exception {
        List<Document> documents = null;
        try {
            hbaseDataStore.saveAll(TEST_APP, documents);
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
            hbaseDataStore.saveAll(TEST_APP, documents);
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
            hbaseDataStore.saveAll(TEST_APP, documents);
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
            hbaseDataStore.saveAll(TEST_APP, documents);
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
        hbaseDataStore.saveAll(TEST_APP, documents);
        verify(tableInterface, times(1)).close();
    }

    public void validateSave(String id, Document expectedDocument) throws Exception {
        Get get = new Get(Bytes.toBytes(id));
        Result result = tableInterface.get(get);
        assertNotNull("Get for Id should not be null", result);
        Document actualDocument = new Document(expectedDocument.getId(),
                expectedDocument.getTimestamp(),
                mapper.readTree(result.getValue(COLUMN_FAMILY, DATA_FIELD_NAME)));
        compare(expectedDocument, actualDocument);
    }

    private String v1FormatKey(String id) {
        return String.format("%s:%s", id, TEST_APP_NAME);
    }

    @Test
    public void testGetSingle() throws Exception {
        // rawKeyVersion 1.0 with no metadata stored in the system (This will happen for documents which were indexed
        // before rawKey versioning came into place)
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection,
                mapper,
                new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1()));

        String id = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document expectedDocument = new Document(id, timestamp, data);
        tableInterface.put(new Put(Bytes.toBytes(v1FormatKey(id)))
                .add(COLUMN_FAMILY, Bytes.toBytes("data"), mapper.writeValueAsBytes(data))
                .add(COLUMN_FAMILY, Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp)));
        Document actualDocument = hbaseDataStore.get(TEST_APP, id);
        compare(expectedDocument, actualDocument);

        // rawKeyVersion 1.0
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection,
                mapper,
                new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1()));

        id = UUID.randomUUID().toString();
        data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        String newId = v1FormatKey(id);
        expectedDocument = new Document(id, System.currentTimeMillis(), new DocumentMetadata(id, newId), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(expectedDocument));
        actualDocument = hbaseDataStore.get(TEST_APP, id);
        compare(expectedDocument, actualDocument);


        // rawKeyVersion 2.0
        DocumentTranslator documentTranslator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV2());
        hbaseDataStore = new HBaseDataStore(hbaseTableConnection, mapper, documentTranslator);

        id = UUID.randomUUID().toString();
        data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document originalDocument = new Document(id, System.currentTimeMillis(), data);
        newId = documentTranslator.translate(TEST_APP, originalDocument).getId();
        expectedDocument = new Document(newId, originalDocument.getTimestamp(), new DocumentMetadata(id, newId), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(expectedDocument));
        actualDocument = hbaseDataStore.get(TEST_APP, newId);
        compare(originalDocument, actualDocument);

    }

    @Test
    public void testGetSingleMissingDocument() {
        try {
            hbaseDataStore.get(TEST_APP, UUID.randomUUID().toString());
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, ex.getCode());
        }
    }

    @Test
    public void testGetSingleHBaseReadException() throws Exception {
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id,
                System.currentTimeMillis(),
                new DocumentMetadata(id, v1FormatKey(id)),
                data);
        tableInterface.put(hbaseDataStore.getPutForDocument(expectedDocument));
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.<Get>any());
        try {
            hbaseDataStore.get(TEST_APP, id);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGetSingleHBaseCloseException() throws Exception {
        Document originalDocument = createDummyDocument();
        originalDocument.setMetadata(new DocumentMetadata(originalDocument.getId(), v1FormatKey(originalDocument.getId())));
        Document expectedDocument = new Document(v1FormatKey(originalDocument.getId()),
                originalDocument.getTimestamp(),
                originalDocument.getMetadata(),
                originalDocument.getData());

        tableInterface.put(hbaseDataStore.getPutForDocument(expectedDocument));
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.get(TEST_APP, originalDocument.getId());
        verify(tableInterface, times(1)).close();
    }

    @Test
    public void testV1GetBulk() throws Exception {
        Map<String, Document> idValues = Maps.newHashMap();
        List<String> ids = new Vector<>();
        List<Put> putList = new Vector<>();
        HashMap<String, Document> actualIdValues = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"));
            String rawId = v1FormatKey(id);
            ids.add(id);
            Document document = new Document(rawId, timestamp,
                    new DocumentMetadata(id, rawId), data);
            putList.add(hbaseDataStore.getPutForDocument(document));
            idValues.put(id, new Document(id, timestamp, data));
        }
        tableInterface.put(putList);
        List<Document> actualDocuments = hbaseDataStore.getAll(TEST_APP, ids);
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
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV2());

        Map<String, Document> idValues = Maps.newHashMap();
        List<String> ids = Lists.newArrayList();
        List<String> rawIds = Lists.newArrayList();
        List<Put> putList = Lists.newArrayList();

        HashMap<String, Document> actualIdValues = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            Document document = createDummyDocument();
            ids.add(document.getId());
            idValues.put(document.getId(), document);

            Document translated = translator.translate(TEST_APP, document);
            putList.add(hbaseDataStore.getPutForDocument(translated));

            rawIds.add(translated.getId());
        }
        tableInterface.put(putList);
        List<Document> actualDocuments = hbaseDataStore.getAll(TEST_APP, rawIds);
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
            hbaseDataStore.getAll(TEST_APP, null);
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
            hbaseDataStore.getAll(TEST_APP, ids);
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
            putList.add(hbaseDataStore.getPutForDocument(document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.anyListOf(Get.class));
        try {
            hbaseDataStore.getAll(TEST_APP, ids);
            fail();
        } catch (FoxtrotException ex) {
            assertEquals(ErrorCode.STORE_CONNECTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGetBulkHBaseCloseException() throws Exception {
        List<String> ids = new Vector<>();
        List<Put> putList = new Vector<>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(),
                    new DocumentMetadata(id, String.format("row:%d", i)),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST")));
            putList.add(hbaseDataStore.getPutForDocument(document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.getAll(TEST_APP, ids);
        verify(tableInterface, times(1)).close();
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

    private Document createDummyDocument() {
        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        document.setData(data);
        return document;
    }
}
