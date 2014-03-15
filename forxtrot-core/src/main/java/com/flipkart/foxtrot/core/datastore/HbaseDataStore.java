package com.flipkart.foxtrot.core.datastore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:54 PM
 */
public class HbaseDataStore implements DataStore {
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DATA_FIELD_NAME = Bytes.toBytes("data");

    private HbaseTableConnection tableWrapper;
    private ObjectMapper mapper;

    public HbaseDataStore(HbaseTableConnection tableWrapper, ObjectMapper mapper) {
        this.tableWrapper = tableWrapper;
        this.mapper = mapper;
    }

    @Override
    public void save(final String table, Document document) throws DataStoreException {
        try {
            tableWrapper.getTable().put(getPutForDocument(table, document));
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_SAVE,
                                                        "Saving document error: " + t.getMessage(), t);
        }
    }

    @Override
    public void save(final String table, List<Document> documents) throws DataStoreException {
        try {
            List<Put> puts = new Vector<Put>();
            for(Document document : documents) {
                puts.add(getPutForDocument(table, document));
            }
            tableWrapper.getTable().put(puts);
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_SAVE,
                    "Saving document error: " + t.getMessage(), t);
        }
    }

    @Override
    public Document get(final String table, String id) throws DataStoreException {
        try {
            Get get = new Get(Bytes.toBytes(id + ":" + table))
                    .addColumn(COLUMN_FAMILY, DATA_FIELD_NAME)
                    .setMaxVersions(1);
            Result getResult = tableWrapper.getTable().get(get);
            if(!getResult.isEmpty()) {
                byte[] data = getResult.getValue(COLUMN_FAMILY, DATA_FIELD_NAME);
                if(null != data) {
                    return mapper.readValue(data, Document.class);
                }
            }
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_GET,
                                t.getMessage(), t);
        }
        throw new DataStoreException(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_ID,
                            String.format("No data found for ID: %s", id));
    }

    @Override
    public List<Document> get(final String table, List<String> ids) throws DataStoreException {
        try {
            List<Get> gets = new ArrayList<Get>(ids.size());
            for(String id: ids) {
                Get get = new Get(Bytes.toBytes(id + ":" + table))
                    .addColumn(COLUMN_FAMILY, DATA_FIELD_NAME)
                    .setMaxVersions(1);
                gets.add(get);
            }
            Result[] getResults = tableWrapper.getTable().get(gets);
            List<Document> results = new ArrayList<Document>(ids.size());
            for(int index = 0; index < getResults.length; index++) {
                boolean found = false;
                Result getResult = getResults[index];
                if(!getResult.isEmpty()) {
                    byte[] data = getResult.getValue(COLUMN_FAMILY, DATA_FIELD_NAME);
                    if(null != data) {
                        results.add(mapper.readValue(data, Document.class));
                        found = true;
                    }
                }
                if(!found)
                {
                    throw new DataStoreException(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_IDS,
                            String.format("No data found for ID: %s", ids.get(index)));
                }
            }
            if(results.isEmpty()) {
                throw new DataStoreException(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_IDS,
                        String.format("No data found for ID: %s", mapper.writeValueAsString(ids)));
            }
            return results;
        } catch (Throwable t) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_GET,
                                            t.getMessage(), t);
        }
    }

    private Put getPutForDocument(final String table, Document document) throws Throwable {
        return new Put(Bytes.toBytes(document.getId() + ":" + table))
                    .add(COLUMN_FAMILY, DATA_FIELD_NAME, mapper.writeValueAsBytes(document));
    }
}
