package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 15/04/14.
 */

@RunWith(MockitoJUnitRunner.class)
public class HbaseDataStoreTest {
    private HbaseDataStore hbaseDataStore;
    private HTableInterface tableInterface;
    private HBaseTestingUtility utility;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DATA_FIELD_NAME = Bytes.toBytes("data");
    private static final String TEST_APP = "test-app";


    @Before
    public void setUp() throws Exception {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setTableName("foxtrot");

        utility = new HBaseTestingUtility();
        startCluster(utility);
        createTables(utility, hbaseConfig);

        this.tableInterface = getTable(hbaseConfig.getTableName(), utility);
        HbaseTableConnection tableConnection = Mockito.mock(HbaseTableConnection.class);
        when(tableConnection.getTable()).thenReturn(tableInterface);
        hbaseDataStore = new HbaseDataStore(tableConnection, mapper);
    }

    @After
    public void tearDown() throws Exception {
        shutdownCluster(utility);
    }

    @Test
    public void testSaveSingle() throws Exception {
        Document expectedDocument = new Document();
        expectedDocument.setId(UUID.randomUUID().toString());
        expectedDocument.setTimestamp(System.currentTimeMillis());
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));
        expectedDocument.setData(data);
        hbaseDataStore.save(TEST_APP, expectedDocument);
        validateSave(expectedDocument);
    }

    @Test
    public void testSaveBulk() throws Exception {
        List<Document> documents = new Vector<Document>();
        for( int i = 0 ; i < 10; i++){
            documents.add(new Document(UUID.randomUUID().toString(),
                    mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST")) ));
        }
        hbaseDataStore.save(TEST_APP, documents);
        for (Document document : documents){
            validateSave(document);
        }
    }

    public void validateSave(Document savedDocument) throws Exception {
        String rowkey = String.format("%s:%s", savedDocument.getId(), TEST_APP);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = tableInterface.get(get);
        assertNotNull("Get for Id should not be null", result);
        Document actualDocument = new Document(savedDocument.getId(),
                mapper.readTree(result.getValue(COLUMN_FAMILY, DATA_FIELD_NAME)));
        compare(savedDocument, actualDocument);
    }

    @Test
    public void testGetSingle() throws Throwable {
        String id = UUID.randomUUID().toString();
        JsonNode data = mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"));

        Document expectedDocument = new Document(id, data);
        tableInterface.put(hbaseDataStore.getPutForDocument(TEST_APP, expectedDocument));
        Document actualDocument = hbaseDataStore.get(TEST_APP, id);
        compare(expectedDocument, actualDocument);
    }

    @Test
    public void testGetBulk() throws Throwable {
        Map<String, Document> idValues = new HashMap<String, Document>();
        List<String> ids = new Vector<String>();
        List<Put> putList = new Vector<Put>();
        for( int i = 0 ; i < 10; i++){
            String id = UUID.randomUUID().toString();
            ids.add(id);
            idValues.put(id,
                    new Document(id, mapper.valueToTree(Collections.singletonMap("TEST_NAME", "SINGLE_SAVE_TEST"))));
            putList.add(hbaseDataStore.getPutForDocument(TEST_APP, idValues.get(id)));
        }
        tableInterface.put(putList);

        List<Document> actualDocuments = hbaseDataStore.get(TEST_APP, ids);
        HashMap<String, Document> actualIdValues = new HashMap<String, Document>();
        for ( Document doc : actualDocuments) {
            actualIdValues.put(doc.getId(), doc);
        }
        assertNotNull("List of returned Documents should not be null", actualDocuments);
        for (String id : ids){
            assertTrue("Requested Id should be present in response", actualIdValues.containsKey(id));
            compare(idValues.get(id), actualIdValues.get(id));
        }
    }

    public void startCluster(HBaseTestingUtility utility) throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
    }

    public void shutdownCluster(HBaseTestingUtility utility) throws Exception {
        utility.shutdownMiniCluster();
    }

    public void createTables(final HBaseTestingUtility utility, final HbaseConfig hbaseConfig) throws Exception {
        HBaseAdmin admin = utility.getHBaseAdmin();

        List<String> families = new ArrayList<String>();
        String tableName = hbaseConfig.getTableName();
        families.add(Bytes.toString(COLUMN_FAMILY));

        byte[] table = Bytes.toBytes(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        for (String family : families) {
            hTableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        if (!admin.tableExists(table)) {
            admin.createTable(hTableDescriptor);
        }
    }

    public HTableInterface getTable(String tableName, HBaseTestingUtility utility){
        HTableFactory hTableFactory = new HTableFactory();
        return hTableFactory.createHTableInterface(utility.getConfiguration(), Bytes.toBytes(tableName));
    }

    public void compare(Document expected, Document actual) throws Exception{
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        byte[] expectedData = mapper.writeValueAsBytes(expected.getData());
        byte[] actualData = mapper.writeValueAsBytes(actual.getData());
        assertEquals("Actual data should match expected data", expectedData, actualData);
    }
}
