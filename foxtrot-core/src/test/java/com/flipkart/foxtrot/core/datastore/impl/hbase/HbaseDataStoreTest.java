package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 15/04/14.
 */

public class HbaseDataStoreTest {
    private HbaseDataStore hbaseDataStore;
    private HTableInterface tableInterface;
    private HbaseTableConnection hBaseTableConnection;
    private ObjectMapper mapper = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(HbaseDataStoreTest.class.getSimpleName());
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DATA_FIELD_NAME = Bytes.toBytes("data");
    private static final String TEST_APP = "test-app";

    @Before
    public void setUp() throws Exception {
        tableInterface = MockHTable.create();
        tableInterface = spy(tableInterface);
        hBaseTableConnection = Mockito.mock(HbaseTableConnection.class);
        when(hBaseTableConnection.getTable()).thenReturn(tableInterface);
        hbaseDataStore = new HbaseDataStore(hBaseTableConnection, mapper);
    }

    @Test
    public void testSaveSingle() throws Exception {
        logger.info("Testing Single Save");
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        hbaseDataStore.save(TEST_APP, expectedDocument);
        validateSave(expectedDocument);
        logger.info("Tested Single Save");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveSingleNullDocument() throws Exception {
        logger.info("Testing Single Save - Null document");
        Document document = null;
        hbaseDataStore.save(TEST_APP, document);
        logger.info("Tested Single Save - Null document");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveSingleNullId() throws Exception {
        logger.info("Testing Single Save - Null Id");
        Document document = new Document(null, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
        hbaseDataStore.save(TEST_APP, document);
        logger.info("Tested Single Save - Null Id");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveSingleNullData() throws Exception {
        logger.info("Testing Single Save - Null Data");
        Document document = new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null);
        hbaseDataStore.save(TEST_APP, document);
        logger.info("Tested Single Save - Null Data");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveSingleHBaseWriteException() throws Throwable {
        logger.info("Testing Single Save - HBase Write Exception");
        Document document = new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));
        doThrow(new IOException())
                .when(tableInterface)
                .put(Matchers.<Put>any());
        hbaseDataStore.save(TEST_APP, document);
        logger.info("Tested Single Save - HBase Write Exception");
    }

    @Test
    public void testSaveSingleHBaseCloseException() throws Throwable {
        logger.info("Testing Single Save - HBase Close Exception");
        Document document = new Document(UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")));

        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.save(TEST_APP, document);
        logger.info("Tested Single Save - HBase Close Exception");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveSingleNullHBaseTableConnection() throws Throwable {
        logger.info("Testing Single Save - Null HBaseTableConnection");
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        when(hBaseTableConnection.getTable()).thenReturn(null);
        hbaseDataStore.save(TEST_APP, expectedDocument);
        logger.info("Tested Single Save - Null HBaseTableConnection");
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
        hbaseDataStore.save(TEST_APP, documents);
        for (Document document : documents) {
            validateSave(document);
        }
        logger.info("Tested Bulk Save");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkNullDocuments() throws Exception {
        logger.info("Testing Bulk Save - Null Documents");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(null);
        }
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - Null Documents");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkNullIdList() throws Exception {
        logger.info("Testing Bulk Save - Null List");
        List<Document> documents = null;
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - Null List");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkNullId() throws Exception {
        logger.info("Testing Bulk Save - Null Id");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(null, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
        }
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - Null Id");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkNullData() throws Exception {
        logger.info("Testing Bulk Save - Null Data");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
        }
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - Null Data");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkHBaseWriteException() throws Exception {
        logger.info("Testing Bulk Save - HBase Write Exception");
        List<Document> documents = new Vector<Document>();
        doThrow(new IOException())
                .when(tableInterface)
                .put(Matchers.anyListOf(Put.class));
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - HBase Write Exception");
    }

    @Test
    public void testSaveBulkHBaseCloseException() throws Exception {
        logger.info("Testing Bulk Save - HBase Close Exception");
        List<Document> documents = new Vector<Document>();
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - HBase Close Exception");
    }

    @Test(expected = DataStoreException.class)
    public void testSaveBulkNullHBaseTableConnection() throws Throwable {
        logger.info("Testing Bulk Save - Null HBaseTableConnection");
        List<Document> documents = new Vector<Document>();
        for (int i = 0; i < 10; i++) {
            documents.add(new Document(UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_SAVE_TEST"))));
        }
        when(hBaseTableConnection.getTable()).thenReturn(null);
        hbaseDataStore.save(TEST_APP, documents);
        logger.info("Tested Bulk Save - Null HBaseTableConnection");
    }

    public void validateSave(Document savedDocument) throws Exception {
        String rowkey = String.format("%s:%s", savedDocument.getId(), TEST_APP);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = tableInterface.get(get);
        assertNotNull("Get for Id should not be null", result);
        Document actualDocument = new Document(savedDocument.getId(),
                savedDocument.getTimestamp(),
                mapper.readTree(result.getValue(COLUMN_FAMILY, DATA_FIELD_NAME)));
        compare(savedDocument, actualDocument);
    }

    @Test
    public void testGetSingle() throws Throwable {
        logger.info("Testing Single Get");
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(TEST_APP, expectedDocument));
        Document actualDocument = hbaseDataStore.get(TEST_APP, id);
        compare(expectedDocument, actualDocument);
        logger.info("Tested Single Get");
    }

    @Test(expected = DataStoreException.class)
    public void testGetSingleMissingDocument() throws Throwable {
        logger.info("Testing Single Get - Missing ID");
        hbaseDataStore.get(TEST_APP, UUID.randomUUID().toString());
        logger.info("Tested Single Get - Missing ID");
    }

    @Test(expected = DataStoreException.class)
    public void testGetSingleHBaseReadException() throws Throwable {
        logger.info("Testing Single Get - HBase Read Exception");
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(TEST_APP, expectedDocument));
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.<Get>any());
        hbaseDataStore.get(TEST_APP, id);
        logger.info("Tested Single Get - HBase Read Exception");
    }

    @Test
    public void testGetSingleHBaseCloseException() throws Throwable {
        logger.info("Testing Single Get - Close Exception");
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(TEST_APP, expectedDocument));
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.get(TEST_APP, id);
        logger.info("Tested Single Get - Close Exception");
    }

    @Test(expected = DataStoreException.class)
    public void testGetSingleNullHBaseTableConnection() throws Throwable {
        logger.info("Testing Single Get - Null HBaseTableConnection");
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        Document expectedDocument = new Document(id, System.currentTimeMillis(), data);
        tableInterface.put(hbaseDataStore.getPutForDocument(TEST_APP, expectedDocument));
        when(hBaseTableConnection.getTable()).thenReturn(null);
        Document actualDocument = hbaseDataStore.get(TEST_APP, id);
        logger.info("Tested Single Get - Null HBaseTableConnection");
    }

    @Test
    public void testGetBulk() throws Throwable {
        logger.info("Testing Bulk Get");
        Map<String, Document> idValues = new HashMap<String, Document>();
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            idValues.put(id,
                    new Document(id, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"))));
            putList.add(hbaseDataStore.getPutForDocument(TEST_APP, idValues.get(id)));
        }
        tableInterface.put(putList);

        List<Document> actualDocuments = hbaseDataStore.get(TEST_APP, ids);
        HashMap<String, Document> actualIdValues = new HashMap<String, Document>();
        for (Document doc : actualDocuments) {
            actualIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", actualDocuments);
        for (String id : ids) {
            assertTrue("Requested Id should be present in response", actualIdValues.containsKey(id));
            compare(idValues.get(id), actualIdValues.get(id));
        }
        logger.info("Tested Bulk Get");
    }

    @Test(expected = DataStoreException.class)
    public void testGetBulkNullIdList() throws Throwable {
        logger.info("Testing Bulk Get - Null ID List");
        List<String> ids = null;
        hbaseDataStore.get(TEST_APP, ids);
        logger.info("Tested Bulk Get - Null ID List");
    }

    @Test(expected = DataStoreException.class)
    public void testGetBulkMissingDocument() throws Throwable {
        logger.info("Testing Bulk Get - Missing ID");
        hbaseDataStore.get(TEST_APP, Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        logger.info("Tested Bulk Get - Missing ID");
    }

    @Test(expected = DataStoreException.class)
    public void testGetBulkHBaseReadException() throws Throwable {
        logger.info("Testing Bulk Get - HBase Read Exception");
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST")));
            putList.add(hbaseDataStore.getPutForDocument(TEST_APP, document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .get(Matchers.anyListOf(Get.class));
        hbaseDataStore.get(TEST_APP, ids);
        logger.info("Tested Bulk Get - Exception");
    }

    @Test
    public void testGetBulkHBaseCloseException() throws Throwable {
        logger.info("Testing Bulk Get - HBase Close Exception");
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            Document document = new Document(id, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST")));
            putList.add(hbaseDataStore.getPutForDocument(TEST_APP, document));
        }
        tableInterface.put(putList);
        doThrow(new IOException())
                .when(tableInterface)
                .close();
        hbaseDataStore.get(TEST_APP, ids);
        logger.info("Tested Bulk Get - HBase Close Exception");
    }

    @Test(expected = DataStoreException.class)
    public void testGetBulkNullHBaseTableConnection() throws Throwable {
        logger.info("Testing Bulk Get - NullHBaseTableConnection");
        Map<String, Document> idValues = new HashMap<String, Document>();
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for (int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            idValues.put(id,
                    new Document(id, System.currentTimeMillis(), mapper.valueToTree(Collections.singletonMap("TEST_NAME", "BULK_GET_TEST"))));
            putList.add(hbaseDataStore.getPutForDocument(TEST_APP, idValues.get(id)));
        }
        tableInterface.put(putList);
        when(hBaseTableConnection.getTable()).thenReturn(null);
        hbaseDataStore.get(TEST_APP, ids);
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
