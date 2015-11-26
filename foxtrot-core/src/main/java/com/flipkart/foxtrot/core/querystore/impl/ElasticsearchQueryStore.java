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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.*;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.parsers.ElasticsearchMappingParser;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.google.common.base.Stopwatch;
import com.google.common.collect.*;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */
public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());

    private final TableMetadataManager tableMetadataManager;
    private final ElasticsearchConnection connection;
    private final DataStore dataStore;
    private final ObjectMapper mapper;


    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore) {
        this.tableMetadataManager = tableMetadataManager;
        this.connection = connection;
        this.dataStore = dataStore;
        this.mapper = ElasticsearchUtils.getMapper();
    }

    @Override
    public void save(String table, Document document) throws QueryStoreException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            if (new DateTime().plusDays(1).minus(document.getTimestamp()).getMillis() <  0) {
                return;
            }
            final Table tableMeta = tableMetadataManager.get(table);
            final Document translatedDocuement = dataStore.save(tableMeta, document);
            long timestamp = translatedDocuement.getTimestamp();

            //translatedDocuement.getData().
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();
            connection.getClient()
                    .prepareIndex()
                    .setIndex(ElasticsearchUtils.getCurrentIndex(table, timestamp))
                    .setType(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setId(translatedDocuement.getId())
                    .setTimestamp(Long.toString(timestamp))
                    .setSource(convert(translatedDocuement))
                    .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                    .execute()
                    .get(2, TimeUnit.SECONDS);
            logger.info(String.format("ES took : %d table : %s", stopwatch.elapsedMillis(), table));
        } catch (QueryStoreException ex) {
            throw ex;
        } catch (DataStoreException ex) {
            DataStoreException.ErrorCode code = ex.getErrorCode();
            if (code.equals(DataStoreException.ErrorCode.STORE_INVALID_REQUEST)
                    || code.equals(DataStoreException.ErrorCode.STORE_INVALID_DOCUMENT)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST,
                        ex.getMessage(), ex);
            } else {
                throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                        ex.getMessage(), ex);
            }
        } catch (JsonProcessingException ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST,
                    ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                    ex.getMessage(), ex);
        }
    }

    @Override
    public void save(String table, List<Document> documents) throws QueryStoreException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            if (documents == null || documents.size() == 0) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST,
                        "Invalid Document List");
            }
            final Table tableMeta = tableMetadataManager.get(table);
            final List<Document> translatedDocuments = dataStore.save(tableMeta, documents);
            BulkRequestBuilder bulkRequestBuilder = connection.getClient().prepareBulk();

            DateTime dateTime = new DateTime().plusDays(1);

            for (Document document : translatedDocuments) {
                long timestamp = document.getTimestamp();
                if (dateTime.minus(timestamp).getMillis() <  0) {
                    continue;
                }
                final String index = ElasticsearchUtils.getCurrentIndex(table, timestamp);
                IndexRequest indexRequest = new IndexRequest()
                        .index(index)
                        .type(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                        .id(document.getId())
                        .timestamp(Long.toString(timestamp))
                        .source(convert(document));
                bulkRequestBuilder.add(indexRequest);
            }
            if (bulkRequestBuilder.numberOfActions() > 0){
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.start();
                BulkResponse responses = bulkRequestBuilder
                        .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                        .execute()
                        .get(10, TimeUnit.SECONDS);
                logger.info(String.format("ES took : %d table : %s", stopwatch.elapsedMillis(), table));
                int failedCount = 0;
                for (int i = 0; i < responses.getItems().length; i++) {
                    BulkItemResponse itemResponse = responses.getItems()[i];
                    failedCount += (itemResponse.isFailed() ? 1 : 0);
                    if (itemResponse.isFailed()) {
                        logger.error(String.format("Table : %s Failure Message : %s Document : %s", table, itemResponse.getFailureMessage(), mapper.writeValueAsString(documents.get(i))));
                    }
                }
                if (failedCount > 0) {
                    logger.error(String.format("Table : %s Failed Documents : %d", table, failedCount));
                }
            }
        } catch (QueryStoreException ex) {
            throw ex;
        } catch (DataStoreException ex) {
            DataStoreException.ErrorCode code = ex.getErrorCode();
            if (code.equals(DataStoreException.ErrorCode.STORE_INVALID_REQUEST)
                    || code.equals(DataStoreException.ErrorCode.STORE_INVALID_DOCUMENT)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST,
                        ex.getMessage(), ex);
            } else {
                throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                        ex.getMessage(), ex);
            }
        } catch (JsonProcessingException ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST,
                    ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                    ex.getMessage(), ex);
        }
    }

    @Override
    public Document get(String table, String id) throws QueryStoreException {
        table = ElasticsearchUtils.getValidTableName(table);
        Table fxTable = null;
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            fxTable = tableMetadataManager.get(table);
        } catch (Exception ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    ex.getMessage(), ex);
        }
        String lookupKey = null;
        try {
            SearchResponse searchResponse = connection.getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(table))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setQuery(
                            QueryBuilders.constantScoreQuery(
                                    FilterBuilders.boolFilter()
                                            .must(FilterBuilders.termFilter(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, id))))
                    .setNoFields()
                    .setSize(1)
                    .execute()
                    .actionGet();
            if(searchResponse.getHits().totalHits() == 0 ) {
                logger.warn("Going into compatibility mode, looks using passed in ID as the data store id: {}", id);
                lookupKey = id;
            }
            else {
                lookupKey = searchResponse.getHits().getHits()[0].getId();
                logger.debug("Translated lookup key for {} is {}.", id, lookupKey);
            }
            return dataStore.get(fxTable, lookupKey);
        } catch (DataStoreException ex) {
            if (ex.getErrorCode().equals(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_ID)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND,
                        ex.getMessage(), ex);
            }
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    ex.getMessage(), ex);
        }

    }

    @Override
    public List<Document> get(String table, List<String> ids) throws QueryStoreException {
        return get(table, ids, false);
    }

    @Override
    public List<Document> get(String table, List<String> ids, boolean bypassMetalookup) throws QueryStoreException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = connection.getClient()
                    .admin().indices().getMappings(
                            new GetMappingsRequest()
                                    .indices(ElasticsearchUtils.getIndices(table))
                                    .types("metadata"))
                    .actionGet()
                    .mappings();

            Map<String, String> rowKeys = Maps.newLinkedHashMap();
            for(String id: ids) {
                rowKeys.put(id, id);
            }
            if(!bypassMetalookup) {
                SearchResponse response = connection.getClient().prepareSearch(ElasticsearchUtils.getIndices(table))
                        .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                        .setQuery(
                                QueryBuilders.constantScoreQuery(
                                        FilterBuilders.inFilter(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, ids.toArray(new String[ids.size()]))))
                        .setFetchSource(false)
                        .addField(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME) //Used for compatibility
                        .setSize(ids.size())
                        .execute()
                        .actionGet();
                Set<String> inputKeys = ImmutableSet.copyOf(ids);
                Set<String> foundKeys = Sets.newHashSet();
                for (SearchHit hit : response.getHits()) {
                    final String id = hit.getFields().get(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME).getValue().toString();
                    rowKeys.put(id,hit.getId());
                    foundKeys.add(id);
                }
            }
            logger.info("Get row keys: {}", rowKeys.size());
            return dataStore.get(tableMetadataManager.get(table), ImmutableList.copyOf(rowKeys.values()));
        } catch (DataStoreException ex) {
            if (ex.getErrorCode().equals(DataStoreException.ErrorCode.STORE_NO_DATA_FOUND_FOR_IDS)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_NOT_FOUND,
                        ex.getMessage(), ex);
            }
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    ex.getMessage(), ex);
        }
    }

    @Override
    public TableFieldMapping getFieldMappings(String table) throws QueryStoreException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }

            ElasticsearchMappingParser mappingParser = new ElasticsearchMappingParser(mapper);
            Set<FieldTypeMapping> mappings = new HashSet<FieldTypeMapping>();
            GetMappingsResponse mappingsResponse = connection.getClient().admin()
                    .indices().prepareGetMappings(ElasticsearchUtils.getIndices(table)).execute().actionGet();

            for (ObjectCursor<String> index : mappingsResponse.getMappings().keys()) {
                MappingMetaData mappingData = mappingsResponse.mappings().get(index.value).get(ElasticsearchUtils.DOCUMENT_TYPE_NAME);
                mappings.addAll(mappingParser.getFieldMappings(mappingData));
            }
            return new TableFieldMapping(table, mappings);
        } catch (QueryStoreException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.METADATA_FETCH_ERROR,
                    ex.getMessage(), ex);
        }
    }

    @Override
    public void cleanupAll() throws QueryStoreException {
        Set<String> tables = new HashSet<String>();
        try {
            for (Table table : tableMetadataManager.get()) {
                tables.add(table.getName());
            }
        } catch (Exception ex) {
            logger.error("Unable to fetch table names for deletion.", ex);
            throw new QueryStoreException(QueryStoreException.ErrorCode.TABLE_LIST_FETCH_ERROR,
                    "Unable to fetch table names for deletion", ex);
        }
        cleanup(tables);
    }

    @Override
    public void cleanup(final String table) throws QueryStoreException {
        cleanup(new HashSet<String>(Arrays.asList(table)));
    }

    @Override
    public void cleanup(Set<String> tables) throws QueryStoreException {
        List<String> indicesToDelete = new ArrayList<String>();
        try {
            IndicesStatusResponse response = connection.getClient().admin().indices().prepareStatus().execute().actionGet();
            Set<String> currentIndices = response.getIndices().keySet();

            for (String currentIndex : currentIndices) {
                String table = ElasticsearchUtils.getTableNameFromIndex(currentIndex);
                if (table != null && tables.contains(table)) {
                    boolean indexEligibleForDeletion;
                    try {
                        indexEligibleForDeletion = ElasticsearchUtils.isIndexEligibleForDeletion(currentIndex, tableMetadataManager.get(table));
                        if (indexEligibleForDeletion) {
                            logger.warn(String.format("Index eligible for deletion : %s", currentIndex));
                            indicesToDelete.add(currentIndex);
                        }
                    } catch (Exception ex) {
                        logger.error(String.format("Unable to Get Table details for Table : %s", table), ex);
                    }
                }
            }
            logger.warn(String.format("Deleting Indexes - Indexes - %s", indicesToDelete));
            if (indicesToDelete.size() > 0) {
                List<List<String>> subLists = Lists.partition(indicesToDelete, 5);
                for ( List<String> subList : subLists ){
                    try {
                        connection.getClient().admin().indices().prepareDelete(subList.toArray(new String[subList.size()]))
                                .execute().actionGet(TimeValue.timeValueMinutes(5));
                        logger.warn(String.format("Deleted Indexes - Indexes - %s", subList));
                    } catch (Exception e){
                        logger.error(String.format("Index deletion failed - Indexes - %s", subList), e);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(String.format("Unable to delete Indexes - %s", indicesToDelete), ex);
            throw new QueryStoreException(QueryStoreException.ErrorCode.DATA_CLEANUP_ERROR,
                    String.format("Unable to delete Indexes - %s", indicesToDelete), ex);
        }
    }

    private String convert(Document translatedDocuement) {
        JsonNode metaNode = mapper.valueToTree(translatedDocuement.getMetadata());
        ObjectNode dataNode = translatedDocuement.getData().deepCopy();
        dataNode.put(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        return dataNode.toString();
    }


}
