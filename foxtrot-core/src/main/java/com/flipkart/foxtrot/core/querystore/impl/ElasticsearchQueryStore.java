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

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */
@Data
public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());
    private static final String TABLE_META = "tableMeta";
    private static final String DATA_STORE = "dataStore";
    private static final String QUERY_STORE = "queryStore";
    private static final String UNKNOWN_TABLE_ERROR_MESSAGE = "unknown_table table:%s";

    private final ElasticsearchConnection connection;
    private final DataStore dataStore;
    private final TableMetadataManager tableMetadataManager;
    private final List<IndexerEventMutator> mutators;
    private final ObjectMapper mapper;
    private final CardinalityConfig cardinalityConfig;

    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore,
                                   List<IndexerEventMutator> mutators,
                                   ObjectMapper mapper,
                                   CardinalityConfig cardinalityConfig) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.tableMetadataManager = tableMetadataManager;
        this.mutators = mutators;
        this.mapper = mapper;
        this.cardinalityConfig = cardinalityConfig;
    }

    @Override
    @Timed
    public void initializeTable(String table) {
        // Nothing needs to be done here since indexes are created at runtime in elasticsearch
    }

    @Override
    @Timed
    public void save(String table, Document document) {
        table = ElasticsearchUtils.getValidTableName(table);
        Stopwatch stopwatch = Stopwatch.createStarted();
        String action = StringUtils.EMPTY;
        try {
            if(!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
            }
            if(new DateTime().plusDays(1)
                       .minus(document.getTimestamp())
                       .getMillis() < 0) {
                return;
            }
            action = TABLE_META;
            stopwatch.reset()
                    .start();
            final Table tableMeta = tableMetadataManager.get(table);
            logger.info("TableMetaGetTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = DATA_STORE;
            final Document translatedDocument = dataStore.save(tableMeta, document);
            logger.info("DataStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = QUERY_STORE;
            long timestamp = translatedDocument.getTimestamp();
            connection.getClient()
                    .prepareIndex()
                    .setIndex(ElasticsearchUtils.getCurrentIndex(table, timestamp))
                    .setType(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setId(translatedDocument.getId())
                    .setSource(convert(table, translatedDocument))
                    .execute()
                    .get(2, TimeUnit.SECONDS);
            logger.info("QueryStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            MetricUtil.getInstance()
                    .registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            Thread.currentThread()
                    .interrupt();
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public void save(String table, List<Document> documents) {
        table = ElasticsearchUtils.getValidTableName(table);
        Stopwatch stopwatch = Stopwatch.createStarted();
        String action = StringUtils.EMPTY;
        try {
            if(!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
            }
            if(documents == null || documents.isEmpty()) {
                throw FoxtrotExceptions.createBadRequestException(table, "Empty Document List Not Allowed");
            }
            action = TABLE_META;
            stopwatch.reset()
                    .start();
            final Table tableMeta = tableMetadataManager.get(table);
            logger.info("TableMetaGetTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = DATA_STORE;
            final List<Document> translatedDocuments = dataStore.saveAll(tableMeta, documents);
            logger.info("DataStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance()
                    .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset()
                    .start();

            action = QUERY_STORE;
            BulkRequestBuilder bulkRequestBuilder = connection.getClient()
                    .prepareBulk();
            DateTime dateTime = new DateTime().plusDays(1);
            for(Document document : translatedDocuments) {
                long timestamp = document.getTimestamp();
                if(dateTime.minus(timestamp)
                           .getMillis() < 0) {
                    continue;
                }
                final String index = ElasticsearchUtils.getCurrentIndex(table, timestamp);
                IndexRequest indexRequest = new IndexRequest().index(index)
                        .type(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                        .id(document.getId())
                        .source(convert(table, document));
                bulkRequestBuilder.add(indexRequest);
            }
            if(bulkRequestBuilder.numberOfActions() > 0) {
                BulkResponse responses = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                        .execute()
                        .get(10, TimeUnit.SECONDS);
                logger.info("QueryStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                MetricUtil.getInstance()
                        .registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                for(int i = 0; i < responses.getItems().length; i++) {
                    BulkItemResponse itemResponse = responses.getItems()[i];
                    if(itemResponse.isFailed()) {
                        String failedDocument = mapper.writeValueAsString(documents.get(i));
                        logger.error("Table : {} Failure Message : {} Document : {}", table, itemResponse.getFailureMessage(),
                                failedDocument
                        );
                    }
                }
            }
        } catch (JsonProcessingException e) {
            MetricUtil.getInstance()
                    .registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            MetricUtil.getInstance()
                    .registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            Thread.currentThread()
                    .interrupt();
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public Document get(String table, String id) {
        table = ElasticsearchUtils.getValidTableName(table);
        Table fxTable;
        if(!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
        }
        fxTable = tableMetadataManager.get(table);
        String lookupKey;
        SearchResponse searchResponse = connection.getClient()
                .prepareSearch(ElasticsearchUtils.getIndices(table))
                .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .setQuery(boolQuery().filter(termQuery(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, id)))
                .setFetchSource(false)
                .setSize(1)
                .execute()
                .actionGet();
        if(searchResponse.getHits()
                   .getTotalHits().value == 0) {
            logger.warn("Going into compatibility mode, looks using passed in ID as the data store id: {}", id);
            lookupKey = id;
        } else {
            lookupKey = searchResponse.getHits()
                    .getHits()[0].getId();
            logger.debug("Translated lookup key for {} is {}.", id, lookupKey);
        }
        return dataStore.get(fxTable, lookupKey);
    }

    @Override
    public List<Document> getAll(String table, List<String> ids) {
        return getAll(table, ids, false);
    }

    @Override
    @Timed
    public List<Document> getAll(String table, List<String> ids, boolean bypassMetalookup) {
        table = ElasticsearchUtils.getValidTableName(table);
        if(!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
        }
        Map<String, String> rowKeys = Maps.newLinkedHashMap();
        for(String id : ids) {
            rowKeys.put(id, id);
        }
        if(!bypassMetalookup) {
            SearchResponse response = connection.getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(table))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setQuery(boolQuery().filter(
                            termsQuery(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, ids.toArray(new String[ids.size()]))))
                    .setFetchSource(false)
                    .addStoredField(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME) // Used for compatibility
                    .setSize(ids.size())
                    .execute()
                    .actionGet();
            for(SearchHit hit : response.getHits()) {
                final String id = hit.getFields()
                        .get(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME)
                        .getValue()
                        .toString();
                rowKeys.put(id, hit.getId());
            }
        }
        logger.info("Get row keys: {}", rowKeys.size());
        return dataStore.getAll(tableMetadataManager.get(table), ImmutableList.copyOf(rowKeys.values()));
    }

    @Override
    public void cleanupAll() {
        Set<String> tables = tableMetadataManager.get()
                .stream()
                .map(Table::getName)
                .collect(Collectors.toSet());
        cleanup(tables);
    }

    @Override
    @Timed
    public void cleanup(final String table) {
        cleanup(ImmutableSet.of(table));
    }

    @Override
    @Timed
    public void cleanup(Set<String> tables) {
        List<String> indicesToDelete = new ArrayList<>();
        try {
            IndicesStatsResponse response = connection.getClient()
                    .admin()
                    .indices()
                    .prepareStats()
                    .execute()
                    .actionGet();
            Set<String> currentIndices = response.getIndices()
                    .keySet();
            indicesToDelete = getIndicesToDelete(tables, currentIndices);
            deleteIndices(indicesToDelete);
        } catch (Exception e) {
            throw FoxtrotExceptions.createDataCleanupException(String.format("Index Deletion Failed indexes - %s", indicesToDelete), e);
        }
    }

    @Override
    public ClusterHealthResponse getClusterHealth() throws ExecutionException, InterruptedException {
        //Bug as mentioned in https://github.com/elastic/elasticsearch/issues/10574
        return connection.getClient()
                .admin()
                .cluster()
                .prepareHealth()
                .execute()
                .get();
    }

    @Override
    public NodesStatsResponse getNodeStats() throws ExecutionException, InterruptedException {
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
        nodesStatsRequest.clear()
                .jvm(true)
                .os(true)
                .fs(true)
                .indices(true)
                .process(true)
                .breaker(true);
        return connection.getClient()
                .admin()
                .cluster()
                .nodesStats(nodesStatsRequest)
                .actionGet();
    }

    @Override
    public IndicesStatsResponse getIndicesStats() throws ExecutionException, InterruptedException {
        return connection.getClient()
                .admin()
                .indices()
                .prepareStats(ElasticsearchUtils.getAllIndicesPattern())
                .clear()
                .setDocs(true)
                .setStore(true)
                .execute()
                .get();
    }

    @Override
    public TableFieldMapping getFieldMappings(String table) {
        return tableMetadataManager.getFieldMappings(table, false, false);
    }

    private Map<String, Object> convert(String table, Document document) {
        JsonNode metaNode = mapper.valueToTree(document.getMetadata());
        ObjectNode dataNode = document.getData()
                .deepCopy();
        dataNode.set(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        dataNode.set(ElasticsearchUtils.DOCUMENT_TIME_FIELD_NAME, mapper.valueToTree(document.getDate()));
        mutators.forEach(mutator-> mutator.mutate(table, document.getId(), dataNode));
        return ElasticsearchQueryUtils.toMap(mapper, dataNode);
    }

    private void deleteIndices(List<String> indicesToDelete) {
        logger.warn("Deleting Indexes - Indexes - {}", indicesToDelete);
        if(!indicesToDelete.isEmpty()) {
            List<List<String>> subLists = Lists.partition(indicesToDelete, 5);
            for(List<String> subList : subLists) {
                try {
                    connection.getClient()
                            .admin()
                            .indices()
                            .prepareDelete(subList.toArray(new String[0]))
                            .execute()
                            .actionGet(TimeValue.timeValueMinutes(5));
                    logger.warn("Deleted Indexes - Indexes - {}", subList);
                } catch (Exception e) {
                    logger.error("Index deletion failed - Indexes - {}", subList, e);
                }
            }
        }
    }

    private List<String> getIndicesToDelete(Set<String> tables, Set<String> currentIndices) {
        List<String> indicesToDelete = new ArrayList<>();
        for(String currentIndex : currentIndices) {
            String table = ElasticsearchUtils.getTableNameFromIndex(currentIndex);
            if(table != null && tables.contains(table)) {
                boolean indexEligibleForDeletion;
                try {
                    indexEligibleForDeletion = ElasticsearchUtils.isIndexEligibleForDeletion(currentIndex, tableMetadataManager.get(table));
                    if(indexEligibleForDeletion) {
                        logger.warn("Index eligible for deletion : {}", currentIndex);
                        indicesToDelete.add(currentIndex);
                    }
                } catch (Exception ex) {
                    logger.error("Unable to Get Table details for Table : {}", table, ex);
                }
            }
        }
        return indicesToDelete;
    }
}
