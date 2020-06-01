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
package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.foxtrot.flipkart.translator.DocumentTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:54 PM
 */
@Singleton
public class HBaseDataStore implements DataStore {
    private static final Logger logger = LoggerFactory.getLogger(HBaseDataStore.class.getSimpleName());

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    private static final byte[] DOCUMENT_FIELD_NAME = Bytes.toBytes("data");
    private static final byte[] DOCUMENT_META_FIELD_NAME = Bytes.toBytes("metadata");
    private static final byte[] TIMESTAMP_FIELD_NAME = Bytes.toBytes("timestamp");
    private static final byte[] DATE_FIELD_NAME = Bytes.toBytes("date");

    private final HbaseTableConnection tableWrapper;
    private final ObjectMapper mapper;
    private final DocumentTranslator translator;

    @Inject
    public HBaseDataStore(HbaseTableConnection tableWrapper, ObjectMapper mapper, DocumentTranslator translator) {
        this.tableWrapper = tableWrapper;
        this.mapper = mapper;
        this.translator = translator;
    }

    @Override
    @Timed
    public void initializeTable(Table table, boolean forceTableCreate) {
        // Check for existence of HBase table during init to make sure HBase is ready for taking writes
        try {
            boolean isTableAvailable = tableWrapper.isTableAvailable(table);
            if(isTableAvailable) {
                return;
            }
            if(forceTableCreate) {
                tableWrapper.createTable(table);
            } else {
                throw FoxtrotExceptions.createTableInitializationException(table, String.format("Create HBase Table - %s",
                                                                                                tableWrapper.getHBaseTableName(table)
                                                                                               ));
            }
        } catch (IOException e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
    }

    @Override
    @Timed
    public Document save(final Table table, Document document) {
        if(document == null || document.getData() == null || document.getId() == null) {
            throw FoxtrotExceptions.createBadRequestException(table.getName(), "Invalid Input Document");
        }
        Document translatedDocument = null;
        try (org.apache.hadoop.hbase.client.Table hTable = tableWrapper.getTable(table)) {
            translatedDocument = translator.translate(table, document);
            hTable.put(getPutForDocument(translatedDocument));
        } catch (JsonProcessingException e) {
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (IOException e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
        return translatedDocument;
    }

    @Override
    @Timed
    public List<Document> saveAll(final Table table, List<Document> documents) {
        if(documents == null || documents.isEmpty()) {
            throw FoxtrotExceptions.createBadRequestException(table.getName(), "null/empty document list not allowed");
        }
        List<Put> puts = new ArrayList<>();
        ImmutableList.Builder<Document> translatedDocuments = ImmutableList.builder();
        List<String> errorMessages = new ArrayList<>();
        try {
            for(int i = 0; i < documents.size(); i++) {
                Document document = documents.get(i);
                if(!isValidDocument(document, errorMessages, i)) {
                    continue;
                }
                Document translatedDocument = translator.translate(table, document);
                puts.add(getPutForDocument(translatedDocument));
                translatedDocuments.add(translatedDocument);
            }
        } catch (JsonProcessingException e) {
            throw FoxtrotExceptions.createBadRequestException(table, e);
        }
        if(!errorMessages.isEmpty()) {
            throw FoxtrotExceptions.createBadRequestException(table.getName(), errorMessages);
        }

        try (org.apache.hadoop.hbase.client.Table hTable = tableWrapper.getTable(table)) {
            hTable.put(puts);
        } catch (IOException e) {
            logger.error("Error occurred while ingesting event in HBase : ", e);
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
        return translatedDocuments.build();
    }

    private boolean isValidDocument(Document document, List<String> errorMessages, int index) {
        if(document == null) {
            errorMessages.add("null document at index - " + index);
            return false;
        }
        if(document.getId() == null || document.getId()
                .trim()
                .isEmpty()) {
            errorMessages.add("null/empty document id at index - " + index);
            return false;
        }
        if(document.getData() == null) {
            errorMessages.add("null document data at index - " + index);
            return false;
        }
        return true;
    }

    @Override
    @Timed
    public Document get(final Table table, String id) {
        try (org.apache.hadoop.hbase.client.Table hTable = tableWrapper.getTable(table)) {
            Get get = new Get(Bytes.toBytes(translator.rawStorageIdFromDocumentId(table, id))).addColumn(COLUMN_FAMILY, DOCUMENT_FIELD_NAME)
                    .addColumn(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME)
                    .addColumn(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME)
                    .setMaxVersions(1);
            Result getResult = hTable.get(get);
            if(!getResult.isEmpty()) {
                byte[] data = getResult.getValue(COLUMN_FAMILY, DOCUMENT_FIELD_NAME);
                byte[] metadata = getResult.getValue(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME);
                byte[] timestamp = getResult.getValue(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME);
                long time = Bytes.toLong(timestamp);
                DocumentMetadata documentMetadata = (null != metadata) ? mapper.readValue(metadata, DocumentMetadata.class) : null;
                return translator.translateBack(new Document(id, time, documentMetadata, mapper.readTree(data)));
            } else {
                logger.error("ID missing in HBase - {}", id);
                throw FoxtrotExceptions.createMissingDocumentException(table, id);
            }
        } catch (IOException e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
    }

    @Override
    @Timed
    public List<Document> getAll(final Table table, List<String> ids) {
        if(CollectionUtils.isEmpty(ids)) {
            throw FoxtrotExceptions.createBadRequestException(table.getName(), "Empty ID List");
        }
        try (org.apache.hadoop.hbase.client.Table hTable = tableWrapper.getTable(table)) {
            List<Get> gets = new ArrayList<>(ids.size());
            for(String id : ids) {
                Get get = new Get(Bytes.toBytes(translator.rawStorageIdFromDocumentId(table, id))).addColumn(COLUMN_FAMILY,
                                                                                                             DOCUMENT_FIELD_NAME
                                                                                                            )
                        .addColumn(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME)
                        .addColumn(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME)
                        .setMaxVersions(1);
                gets.add(get);
            }
            Result[] getResults = hTable.get(gets);
            List<String> missingIds = new ArrayList<>();
            List<Document> results = new ArrayList<>(ids.size());
            for(int index = 0; index < getResults.length; index++) {
                Result getResult = getResults[index];
                if(!getResult.isEmpty()) {
                    byte[] data = getResult.getValue(COLUMN_FAMILY, DOCUMENT_FIELD_NAME);
                    byte[] metadata = getResult.getValue(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME);
                    byte[] timestamp = getResult.getValue(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME);
                    long time = Bytes.toLong(timestamp);
                    DocumentMetadata documentMetadata = (null != metadata) ? mapper.readValue(metadata, DocumentMetadata.class) : null;
                    final String docId = (null == metadata) ? Bytes.toString(getResult.getRow())
                            .split(":")[0] : documentMetadata.getRawStorageId();
                    results.add(translator.translateBack(new Document(docId, time, documentMetadata, mapper.readTree(data))));
                } else {
                    missingIds.add(ids.get(index));
                }
            }
            if (!missingIds.isEmpty()) {
                logger.error("ID's missing in HBase - {}", missingIds);
                throw FoxtrotExceptions.createMissingDocumentsException(table, missingIds);
            }
            return results;
        } catch (JsonProcessingException e) {
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (IOException e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
    }

    @VisibleForTesting
    public Put getPutForDocument(Document document) throws JsonProcessingException {
        return new Put(Bytes.toBytes(document.getMetadata()
                                             .getRawStorageId())).addColumn(COLUMN_FAMILY, DOCUMENT_META_FIELD_NAME,
                                                                            mapper.writeValueAsBytes(document.getMetadata())
                                                                           )
                .addColumn(COLUMN_FAMILY, DOCUMENT_FIELD_NAME, mapper.writeValueAsBytes(document.getData()))
                .addColumn(COLUMN_FAMILY, TIMESTAMP_FIELD_NAME, Bytes.toBytes(document.getTimestamp()))
                .addColumn(COLUMN_FAMILY, DATE_FIELD_NAME, mapper.writeValueAsBytes(document.getDate()));
    }

    @Override
    @Timed
    public void updateTable(final Table table) {
        // Check for existence of HBase table during update to make sure HBase is ready for taking writes
        try {
            boolean isTableAvailable = tableWrapper.isTableAvailable(table);
            if (isTableAvailable) {
                tableWrapper.updateTable(table);
            }
        }
        catch (IOException e) {
            throw FoxtrotExceptions.createConnectionException(table, e);
        }
    }

}
