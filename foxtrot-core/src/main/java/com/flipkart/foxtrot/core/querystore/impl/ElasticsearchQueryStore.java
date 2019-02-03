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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
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

    private final ElasticsearchConnection connection;
    private final DataStore dataStore;
    private final TableMetadataManager tableMetadataManager;
    private final ObjectMapper mapper;
    private final CardinalityConfig cardinalityConfig;

    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore,
                                   ObjectMapper mapper, CardinalityConfig cardinalityConfig) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.tableMetadataManager = tableMetadataManager;
        this.mapper = mapper;
        this.cardinalityConfig = cardinalityConfig;
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
        Stopwatch stopwatch = Stopwatch.createStarted();
        String action = StringUtils.EMPTY;
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table,
                        String.format("unknown_table table:%s", table));
            }
            if (new DateTime().plusDays(1).minus(document.getTimestamp()).getMillis() < 0) {
                return;
            }
            action = TABLE_META;
            stopwatch.reset().start();
            final Table tableMeta = tableMetadataManager.get(table);
            logger.info("TableMetaGetTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset().start();

            action = DATA_STORE;
            final Document translatedDocument = dataStore.save(tableMeta, document);
            logger.info("DataStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset().start();

            action = QUERY_STORE;
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
            logger.info("QueryStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            MetricUtil.getInstance().registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public void save(String table, List<Document> documents) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        Stopwatch stopwatch = Stopwatch.createStarted();
        String action = StringUtils.EMPTY;
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table,
                        String.format("unknown_table table:%s", table));
            }
            if (documents == null || documents.size() == 0) {
                throw FoxtrotExceptions.createBadRequestException(table, "Empty Document List Not Allowed");
            }
            action = TABLE_META;
            stopwatch.reset().start();
            final Table tableMeta = tableMetadataManager.get(table);
            logger.info("TableMetaGetTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset().start();

            action = DATA_STORE;
            final List<Document> translatedDocuments = dataStore.saveAll(tableMeta, documents);
            logger.info("DataStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            stopwatch.reset().start();

            action = QUERY_STORE;
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
                logger.info("QueryStoreTook:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                MetricUtil.getInstance().registerActionSuccess(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
            MetricUtil.getInstance().registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            MetricUtil.getInstance().registerActionFailure(action, table, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @Override
    @Timed
    public Document get(String table, String id) throws FoxtrotException {
        table = ElasticsearchUtils.getValidTableName(table);
        Table fxTable;
        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createBadRequestException(table,
                    String.format("unknown_table table:%s", table));
        }
        fxTable = tableMetadataManager.get(table);
        String lookupKey;
        SearchResponse searchResponse = connection.getClient()
                .prepareSearch(ElasticsearchUtils.getIndices(table))
                .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .setQuery(boolQuery().filter(termQuery(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, id)))
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
            throw FoxtrotExceptions.createBadRequestException(table,
                    String.format("unknown_table table:%s", table));
        }
        Map<String, String> rowKeys = Maps.newLinkedHashMap();
        for (String id : ids) {
            rowKeys.put(id, id);
        }
        if (!bypassMetalookup) {
            SearchResponse response = connection.getClient().prepareSearch(ElasticsearchUtils.getIndices(table))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setQuery(boolQuery().filter(termsQuery(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, ids.toArray(new String[ids.size()]))))
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
            IndicesStatsResponse response = connection.getClient().admin().indices().prepareStats().execute().actionGet();
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

    @Override
    public TableFieldMapping getFieldMappings(String table) throws FoxtrotException {
        return tableMetadataManager.getFieldMappings(table, false, false);
    }

    private String convert(Document translatedDocument) {
        JsonNode metaNode = mapper.valueToTree(translatedDocument.getMetadata());
        ObjectNode dataNode = translatedDocument.getData().deepCopy();
        dataNode.set(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        return dataNode.toString();
    }

}
