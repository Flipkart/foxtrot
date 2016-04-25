/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.parsers.ElasticsearchMappingParser;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */

public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());

    private final ElasticsearchConnection connection;
    private final DataStore dataStore;
    private final TableMetadataManager tableMetadataManager;
    private final ObjectMapper mapper;

    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore,
                                   ObjectMapper mapper) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.tableMetadataManager = tableMetadataManager;
        this.mapper = mapper;
    }

    @Override
    @Timed
    public void initializeTable(String table) throws FoxtrotException {
        // Nothing needs to be done here since indexes are created at runtime in elasticsearch
    }

    @Override
    @Timed
    public void save(String table, Document document) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createTableMissingException(table);
            }
            if (new DateTime().plusDays(1).minus(document.getTimestamp()).getMillis() < 0) {
                return;
            }
            final Table tableMeta = tableMetadataManager.get(table);
            final Document translatedDocument = dataStore.save(tableMeta, document);
            long timestamp = translatedDocument.getTimestamp();
            connection.getClient()
                    .prepareIndex()
                    .setIndex(ElasticsearchUtils.getCurrentIndex(table, timestamp))
                    .setType(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setId(translatedDocument.getId())
                    .setTimestamp(Long.toString(timestamp))
                    .setSource(convert(translatedDocument))
                    .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                    .execute()
                    .get(2, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public void save(String table, List<Document> documents) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createTableMissingException(table);
            }
            if (documents == null || documents.size() == 0) {
                throw FoxtrotExceptions.createBadRequestException(table, "Empty Document List Not Allowed");
            }
            final Table tableMeta = tableMetadataManager.get(table);
            final List<Document> translatedDocuments = dataStore.saveAll(tableMeta, documents);
            BulkRequestBuilder bulkRequestBuilder = connection.getClient().prepareBulk();

            DateTime dateTime = new DateTime().plusDays(1);

            for (Document document : translatedDocuments) {
                long timestamp = document.getTimestamp();
                if (dateTime.minus(timestamp).getMillis() < 0) {
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
            if (bulkRequestBuilder.numberOfActions() > 0) {
                BulkResponse responses = bulkRequestBuilder
                        .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                        .execute()
                        .get(10, TimeUnit.SECONDS);
                for (int i = 0; i < responses.getItems().length; i++) {
                    BulkItemResponse itemResponse = responses.getItems()[i];
                    if (itemResponse.isFailed()) {
                        logger.error(String.format("Table : %s Failure Message : %s Document : %s", table,
                                itemResponse.getFailureMessage(),
                                mapper.writeValueAsString(documents.get(i))));
                    }
                }
            }
        } catch (JsonProcessingException e) {
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public Document get(String table, String id) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        Table fxTable;
        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createTableMissingException(table);
        }
        fxTable = tableMetadataManager.get(table);
        String lookupKey;
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
        if (searchResponse.getHits().totalHits() == 0) {
            logger.warn("Going into compatibility mode, looks using passed in ID as the data store id: {}", id);
            lookupKey = id;
        } else {
            lookupKey = searchResponse.getHits().getHits()[0].getId();
            logger.debug("Translated lookup key for {} is {}.", id, lookupKey);
        }
        return dataStore.get(fxTable, lookupKey);
    }

    @Override
    public List<Document> getAll(String table, List<String> ids) throws FoxtrotException {
        return getAll(table, ids, false);
    }

    @Override
    @Timed
    public List<Document> getAll(String table, List<String> ids, boolean bypassMetalookup) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createTableMissingException(table);
        }
        Map<String, String> rowKeys = Maps.newLinkedHashMap();
        for (String id : ids) {
            rowKeys.put(id, id);
        }
        if (!bypassMetalookup) {
            SearchResponse response = connection.getClient().prepareSearch(ElasticsearchUtils.getIndices(table))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setQuery(
                            QueryBuilders.constantScoreQuery(
                                    FilterBuilders.inFilter(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, ids.toArray(new String[ids.size()]))))
                    .setFetchSource(false)
                    .addField(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME) // Used for compatibility
                    .setSize(ids.size())
                    .execute()
                    .actionGet();
            for (SearchHit hit : response.getHits()) {
                final String id = hit.getFields().get(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME).getValue().toString();
                rowKeys.put(id, hit.getId());
            }
        }
        logger.info("Get row keys: {}", rowKeys.size());
        return dataStore.getAll(tableMetadataManager.get(table), ImmutableList.copyOf(rowKeys.values()));
    }

    @Override
    @Timed
    public TableFieldMapping getFieldMappings(String table) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);

        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createTableMissingException(table);
        }
        try {
            ElasticsearchMappingParser mappingParser = new ElasticsearchMappingParser(mapper);
            Set<FieldTypeMapping> mappings = new HashSet<>();
            GetMappingsResponse mappingsResponse = connection.getClient().admin()
                    .indices().prepareGetMappings(ElasticsearchUtils.getIndices(table)).execute().actionGet();

            for (ObjectCursor<String> index : mappingsResponse.getMappings().keys()) {
                MappingMetaData mappingData = mappingsResponse.mappings().get(index.value).get(ElasticsearchUtils.DOCUMENT_TYPE_NAME);
                mappings.addAll(mappingParser.getFieldMappings(mappingData));
            }
            return new TableFieldMapping(table, mappings);
        } catch (IOException e) {
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    public void cleanupAll() throws FoxtrotException {
        Set<String> tables = tableMetadataManager.get().stream().map(Table::getName).collect(Collectors.toSet());
        cleanup(tables);
    }

    @Override
    @Timed
    public void cleanup(final String table) throws FoxtrotException {
        cleanup(ImmutableSet.of(table));
    }

    @Override
    @Timed
    public void cleanup(Set<String> tables) throws FoxtrotException {
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
                for (List<String> subList : subLists) {
                    try {
                        connection.getClient().admin().indices().prepareDelete(subList.toArray(new String[subList.size()]))
                                .execute().actionGet(TimeValue.timeValueMinutes(5));
                        logger.warn(String.format("Deleted Indexes - Indexes - %s", subList));
                    } catch (Exception e) {
                        logger.error(String.format("Index deletion failed - Indexes - %s", subList), e);
                    }
                }
            }
        } catch (Exception e) {
            throw FoxtrotExceptions.createDataCleanupException(String.format("Index Deletion Failed indexes - %s", indicesToDelete), e);
        }
    }

    @Override
    public ClusterHealthResponse getClusterHealth() throws ExecutionException, InterruptedException {
        //Bug as mentioned in https://github.com/elastic/elasticsearch/issues/10574
        return connection.getClient().admin().cluster().prepareHealth().execute().get();
    }

    @Override
    public NodesStatsResponse getNodeStats() throws ExecutionException, InterruptedException {
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
        nodesStatsRequest.clear().jvm(true).os(true).fs(true).indices(true).process(true).breaker(true);
        return connection.getClient().admin().cluster().nodesStats(nodesStatsRequest).actionGet();
    }

    @Override
    public IndicesStatsResponse getIndicesStats() throws ExecutionException, InterruptedException {
        return connection.getClient().admin().indices().prepareStats(ElasticsearchUtils.getAllIndicesPattern()).clear().setDocs(true).setStore(true).execute().get();
    }
    private String convert(Document translatedDocument) {
        JsonNode metaNode = mapper.valueToTree(translatedDocument.getMetadata());
        ObjectNode dataNode = translatedDocument.getData().deepCopy();
        dataNode.put(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        return dataNode.toString();
    }

}
