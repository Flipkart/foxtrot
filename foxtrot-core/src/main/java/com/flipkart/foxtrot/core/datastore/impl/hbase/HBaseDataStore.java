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

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.shash.hbase.ds.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:54 PM
 */
public class HBaseDataStore implements DataStore {
    private static final Logger logger = LoggerFactory.getLogger(HBaseDataStore.class.getSimpleName());

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DOCUMENT_FIELD_NAME = Bytes.toBytes("data");
    private static final byte[] DOCUMENT_META_FIELD_NAME = Bytes.toBytes("metadata");
    private static final byte[] TIMESTAMP_FIELD_NAME = Bytes.toBytes("timestamp");

    private final HbaseTableConnection tableWrapper;
    private final ObjectMapper mapper;
    private final DocumentTranslator translator;

    public HBaseDataStore(HbaseTableConnection tableWrapper, ObjectMapper mapper) {
        this.tableWrapper = tableWrapper;
        this.mapper = mapper;
        this.translator = new DocumentTranslator(new RowKeyDistributorByHashPrefix(
                new RowKeyDistributorByHashPrefix.OneByteSimpleHash(tableWrapper.getHbaseConfig().getNumBuckets())));
    }

    @Override
    @Timed
    public void initializeTable(Table table) throws DataStoreException {
        // Check for existence of HBase table during init to make sure HBase is ready for taking writes
        try {
            boolean isTableAvailable = tableWrapper.isTableAvailable(table);
            if (!isTableAvailable) {
                throw new DataStoreException(DataStoreException.ErrorCode.TABLE_NOT_FOUND,
                        "HBase Table Missing. Create HBase table");
            }
        } catch (IOException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_CONNECTION,
                    "HBase Connection Failed", e);
        }
    }

    @Override
    @Timed
    public Document save(final Table table, Document document) throws DataStoreException {
        if (document == null || document.getData() == null || document.getId() == null) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST, "Invalid Document");
        }
        HTableInterface hTable = null;
        Document translatedDocument = null;
        try {
            translatedDocument = translator.translate(table, document);
            hTable = tableWrapper.getTable(table);
            hTable.put(getPutForDocument(table, translatedDocument));
        } catch (JsonProcessingException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST,
                    e.getMessage(), e);
        } catch (IOException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_SAVE,
                    e.getMessage(), e);
        } catch (Exception e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_SAVE,
                    e.getMessage(), e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    logger.error("Error closing table: ", e);
                }
            }
        }
        return translatedDocument;
    }

    @Override
    @Timed
    public List<Document> saveAll(final Table table, List<Document> documents) throws DataStoreException {
        if (documents == null || documents.isEmpty()) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST, "Invalid Documents List");
        }
        List<Put> puts = new Vector<Put>();
        ImmutableList.Builder<Document> translatedDocuments = ImmutableList.builder();
        try {
            for (Document document : documents) {
                if (document == null || document.getData() == null || document.getId() == null) {
                    throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST,
                            "Invalid Document");
                }
                Document translatedDocument = translator.translate(table, document);
                puts.add(getPutForDocument(table, translatedDocument));
                translatedDocuments.add(translatedDocument);
            }
        } catch (JsonProcessingException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST,
                    e.getMessage(), e);
        }

        HTableInterface hTable = null;
        try {
            hTable = tableWrapper.getTable(table);
            hTable.put(puts);
        } catch (IOException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_SAVE,
                    e.getMessage(), e);
        } catch (Exception e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_SAVE,
                    e.getMessage(), e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    logger.error("Error closing table: ", e);
                }
            }
        }
        return translatedDocuments.build();
    }

    @Override
    @Timed
    public Document get(final Table table, String id) throws DataStoreException {
        HTableInterface hTable = null;
        try {
            Get get = new Get(Bytes.toBytes(translator.rawStorageIdFromDocumentId(table, id)))
                    .addColumn(COLUMN_FAMILY, DOCUMENT_FIELD_NAME)
                    .addColumn(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME)
                    .addColumn(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME)
                    .setMaxVersions(1);
            hTable = tableWrapper.getTable(table);
            Result getResult = hTable.get(get);
            if (!getResult.isEmpty()) {
                byte[] data = getResult.getValue(COLUMN_FAMILY, DOCUMENT_FIELD_NAME);
                byte[] metadata = getResult.getValue(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME);
                byte[] timestamp = getResult.getValue(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME);
                long time = Bytes.toLong(timestamp);
                DocumentMetadata documentMetadata = (null != metadata) ? mapper.readValue(metadata, DocumentMetadata.class) : null;
                return translator.translateBack(new Document(id, time, documentMetadata, mapper.readTree(data)));
            } else {
                throw new DataStoreException(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_ID,
                        String.format("No data found for ID: %s", id));
            }
        } catch (DataStoreException ex) {
            throw ex;
        } catch (IOException ex) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_GET,
                    ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_SINGLE_GET,
                    ex.getMessage(), ex);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    logger.error("Error closing table: ", e);
                }
            }
        }
    }

    @Override
    @Timed
    public List<Document> getAll(final Table table, List<String> ids) throws DataStoreException {
        if (ids == null) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST, "Invalid Request IDs");
        }

        HTableInterface hTable = null;
        try {
            List<Get> gets = new ArrayList<Get>(ids.size());
            for (String id : ids) {
                Get get = new Get(Bytes.toBytes(translator.rawStorageIdFromDocumentId(table, id)))
                        .addColumn(COLUMN_FAMILY, DOCUMENT_FIELD_NAME)
                        .addColumn(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME)
                        .addColumn(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME)
                        .setMaxVersions(1);
                gets.add(get);
            }
            hTable = tableWrapper.getTable(table);
            Result[] getResults = hTable.get(gets);
            List<Document> results = new ArrayList<Document>(ids.size());
            for (int index = 0; index < getResults.length; index++) {
                Result getResult = getResults[index];
                if (!getResult.isEmpty()) {
                    byte[] data = getResult.getValue(COLUMN_FAMILY, DOCUMENT_FIELD_NAME);
                    byte[] metadata = getResult.getValue(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME);
                    byte[] timestamp = getResult.getValue(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME);
                    long time = Bytes.toLong(timestamp);
                    DocumentMetadata documentMetadata = (null != metadata)
                                        ? mapper.readValue(metadata, DocumentMetadata.class)
                                        : null;
                    final String docId = (null == metadata)
                                            ? Bytes.toString(getResult.getRow()).split(":")[0]
                                            : documentMetadata.getRawStorageId();
                    results.add(translator.translateBack(new Document(docId, time, documentMetadata, mapper.readTree(data))));
                } else {
                    throw new DataStoreException(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_IDS,
                            String.format("No data found for ID: %s", ids.get(index)));
                }
            }
            return results;
        } catch (DataStoreException ex) {
            throw ex;
        } catch (JsonProcessingException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_INVALID_REQUEST,
                    e.getMessage(), e);
        } catch (IOException e) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_GET,
                    e.getMessage(), e);
        } catch (Exception ex) {
            throw new DataStoreException(DataStoreException.ErrorCode.STORE_MULTI_GET,
                    ex.getMessage(), ex);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    logger.error("Error closing table: ", e);
                }
            }
        }
    }

    @VisibleForTesting
    public Put getPutForDocument(final Table table, Document document) throws JsonProcessingException {
        return new Put(Bytes.toBytes(document.getId()))
                .add(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME, mapper.writeValueAsBytes(document.getMetadata()))
                .add(COLUMN_FAMILY, DOCUMENT_FIELD_NAME, mapper.writeValueAsBytes(document.getData()))
                .add(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME, Bytes.toBytes(document.getTimestamp()));
    }

}
