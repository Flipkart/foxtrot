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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse;
import com.flipkart.foxtrot.common.exception.ElasticsearchQueryStoreException;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.parsers.ElasticsearchTemplateMappingParser;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.appform.functionmetrics.MetricTerm;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.flipkart.foxtrot.common.exception.FoxtrotExceptions.ERROR_DELIMITER;
import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.DOCUMENT_TYPE_NAME;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */
@Data
@Slf4j
@Singleton
@SuppressWarnings("squid:CallToDeprecatedMethod")
public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());
    private static final String TABLE_META = "tableMeta";
    private static final String DATA_STORE = "dataStore";
    private static final String QUERY_STORE = "queryStore";
    private static final String UNKNOWN_TABLE_ERROR_MESSAGE = "unknown_table table:%s";
    private static final String UNKNOWN_TENANT_ERROR_MESSAGE = "unknown_tenant tenant:%s";
    private static final String ERROR_SAVING_DOCUMENTS = "Error while saving documents to table";
    private static final String NODE_FS_STATS_ENDPOINT = "_nodes/stats/fs?format=JSON";
    private static final String LIST_INDICES_ENDPOINT = "_cat/indices/%s?format=JSON&bytes=b";
    private static final String SHARD_STATS_ENDPOINT = "/_cat/shards/%s?format=JSON&bytes=b";

    private final ElasticsearchConnection connection;
    private final DataStore dataStore;
    private final TableMetadataManager tableMetadataManager;
    private final TenantMetadataManager tenantMetadataManager;
    private final List<IndexerEventMutator> mutators;
    private final CardinalityConfig cardinalityConfig;
    private final ElasticsearchTemplateMappingParser templateMappingParser;


    @Inject
    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   TenantMetadataManager tenantMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore,
                                   List<IndexerEventMutator> mutators,
                                   final ElasticsearchTemplateMappingParser templateMappingParser,
                                   CardinalityConfig cardinalityConfig) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.tableMetadataManager = tableMetadataManager;
        this.tenantMetadataManager = tenantMetadataManager;
        this.mutators = mutators;
        this.templateMappingParser = templateMappingParser;
        this.cardinalityConfig = cardinalityConfig;
    }

    private static String bulkSaveFailureMessage(BulkResponse bulkResponse) {
        return Stream.of(bulkResponse.getItems())
                .filter(BulkItemResponse::isFailed)
                .map(BulkItemResponse::getFailureMessage)
                .collect(Collectors.joining(ERROR_DELIMITER));
    }

    @Override
    @Timed
    public void initializeTable(Table table) {
        saveIndexMappingTemplate(table);
    }

    @Override
    @Timed
    public void initializeTenant(String tenant) {
        // Nothing needs to be done here since indexes are created at runtime in elasticsearch
    }

    @Override
    public void updateTable(String tableName,
                            Table table) {
        saveIndexMappingTemplate(table);
    }

    @Override
    @Timed
    @MonitoredFunction
    public void save(@MetricTerm String table,
                     Document document) {
        table = ElasticsearchUtils.getValidName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table,
                        String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
            }
            if (new DateTime().plusDays(1)
                    .minus(document.getTimestamp())
                    .getMillis() < 0) {
                return;
            }
            final Table tableMeta = tableMetadataManager.get(table);

            final Document translatedDocument = dataStore.save(tableMeta.getName(), tableMeta, document);

            saveDocument(table, translatedDocument);

        } catch (IOException e) {
            Thread.currentThread()
                    .interrupt();
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @MonitoredFunction
    private void saveDocument(@MetricTerm String table,
                              Document translatedDocument) throws IOException {
        long timestamp = translatedDocument.getTimestamp();
        IndexRequest indexRequest = new IndexRequest(ElasticsearchUtils.getCurrentIndex(table, timestamp)).type(
                DOCUMENT_TYPE_NAME)
                .id(translatedDocument.getId())
                .source(convert(table, translatedDocument))
                .timeout(new TimeValue(2, TimeUnit.SECONDS));
        getConnection().getClient()
                .index(indexRequest);
    }

    @Override
    @Timed
    @MonitoredFunction
    public void saveAll(@MetricTerm String table,
                        List<Document> documents) {
        table = ElasticsearchUtils.getValidName(table);
        try {
            if (!tableMetadataManager.exists(table)) {
                throw FoxtrotExceptions.createBadRequestException(table,
                        String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
            }
            if (documents == null || documents.isEmpty()) {
                throw FoxtrotExceptions.createBadRequestException(table, "Empty Document List Not Allowed");
            }

            final Table tableMeta = tableMetadataManager.get(table);

            final List<Document> translatedDocuments = dataStore.saveAll(tableMeta.getName(), documents, tableMeta);

            saveAllDocuments(table, documents, translatedDocuments);

        } catch (JsonProcessingException e) {
            logFailedDocuments(table, documents, e);
            throw FoxtrotExceptions.createBadRequestException(table, e);
        } catch (IOException e) {
            logFailedDocuments(table, documents, e);
            Thread.currentThread()
                    .interrupt();
            throw FoxtrotExceptions.createExecutionException(table, e);
        } catch (FoxtrotException e) {
            logFailedDocuments(table, documents, e);
            throw e;
        } catch (Exception e) {
            logFailedDocuments(table, documents, e);
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
    }

    @MonitoredFunction
    private void saveAllDocuments(@MetricTerm String table,
                                  List<Document> documents,
                                  List<Document> translatedDocuments) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        DateTime dateTime = new DateTime().plusDays(1);
        for (Document document : translatedDocuments) {
            long timestamp = document.getTimestamp();
            if (dateTime.minus(timestamp)
                    .getMillis() < 0) {
                continue;
            }
            final String index = ElasticsearchUtils.getCurrentIndex(table, timestamp);
            IndexRequest indexRequest = new IndexRequest().index(index)
                    .type(DOCUMENT_TYPE_NAME)
                    .id(document.getId())
                    .source(convert(table, document));
            bulkRequest.add(indexRequest);
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkRequest.timeout(new TimeValue(10, TimeUnit.SECONDS));
            BulkResponse bulkResponse = getConnection().getClient()
                    .bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {

                printFailedDocuments(table, documents, bulkResponse);

                throw FoxtrotExceptions.createExecutionException(table,
                        new RuntimeException(bulkSaveFailureMessage(bulkResponse)));
            }

        }
    }

    private void printFailedDocuments(String table,
                                      List<Document> documents,
                                      BulkResponse bulkResponse) {
        for (int i = 0; i < bulkResponse.getItems().length; i++) {
            BulkItemResponse itemResponse = bulkResponse.getItems()[i];
            if (itemResponse.isFailed()) {
                String failedDocument = JsonUtils.toJson(documents.get(i));
                log.error("Table : {} Failure Message : {} Document : {}", table, itemResponse.getFailureMessage(),
                        failedDocument);
            }
        }
    }

    private void logFailedDocuments(String table,
                                    List<Document> documents,
                                    Exception e) {
        log.debug("{}: {}, documents :{}, error: {}", ERROR_SAVING_DOCUMENTS, table, documents, e.getMessage());
        log.error("{}: {}", ERROR_SAVING_DOCUMENTS, table, e);
    }

    @Override
    @Timed
    @MonitoredFunction
    public Document get(String table,
                        String id) {
        table = ElasticsearchUtils.getValidName(table);
        Table fxTable;
        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
        }
        fxTable = tableMetadataManager.get(table);
        String lookupKey = getDocument(table, id);
        return dataStore.get(fxTable.getName(), fxTable, lookupKey);
    }

    @MonitoredFunction
    private String getDocument(@MetricTerm String table,
                               String id) {
        String lookupKey;
        val searchRequest = new SearchRequest(ElasticsearchUtils.getIndices(table))
                .types(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                .source(new SearchSourceBuilder()
                        .query(boolQuery().filter(termQuery(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, id)))
                        .fetchSource(false)
                        .size(1));
        SearchResponse searchResponse = null;
        try {
            searchResponse = getConnection().getClient()
                    .search(searchRequest);
        } catch (IOException e) {
            throw FoxtrotExceptions.createExecutionException(table, e);
        }
        if (searchResponse.getHits()
                .getTotalHits() == 0) {
            logger.warn("Going into compatibility mode, looks using passed in ID as the data store id: {}", id);
            lookupKey = id;
        } else {
            lookupKey = searchResponse.getHits()
                    .getHits()[0].getId();
            log.debug("Translated lookup key for {} is {}.", id, lookupKey);
        }
        return lookupKey;
    }

    @Override
    @MonitoredFunction
    public List<Document> getAll(String table,
                                 List<String> ids) {
        return getAll(table, ids, false);
    }

    @Override
    @Timed
    @MonitoredFunction
    public List<Document> getAll(@MetricTerm String table,
                                 List<String> ids,
                                 boolean bypassMetalookup) {
        table = ElasticsearchUtils.getValidName(table);
        if (!tableMetadataManager.exists(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, String.format(UNKNOWN_TABLE_ERROR_MESSAGE, table));
        }
        Map<String, String> rowKeys = Maps.newLinkedHashMap();
        for (String id : ids) {
            rowKeys.put(id, id);
        }
        if (!bypassMetalookup) {
            SearchResponse response = null;
            try {
                response = connection.getClient()
                        .search(new SearchRequest(ElasticsearchUtils.getIndices(table))
                                .types(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                                .source(new SearchSourceBuilder()
                                        .query(boolQuery().filter(termsQuery(
                                                ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME, ids.toArray(new String[ids.size()]))))
                                        .fetchSource(false)
                                        .storedField(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME) // Used for compatibility
                                        .size(ids.size())), RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new ElasticsearchQueryStoreException("Failed to fetch query result from elasticsearch", e);
            }
            for (SearchHit hit : response.getHits()) {
                final String id = hit.getFields()
                        .get(ElasticsearchUtils.DOCUMENT_META_ID_FIELD_NAME)
                        .getValue()
                        .toString();
                rowKeys.put(id, hit.getId());
            }
        }
        log.info("Get row keys: {}", rowKeys.size());
        log.warn("elasticsearch query store :{}", this);
        Table tableMeta = tableMetadataManager.get(table);
        return dataStore.getAll(tableMeta.getName(), tableMeta, ImmutableList.copyOf(rowKeys.values()));
    }

    @Override
    public Set<String> getTablesIndices() {
        try {
            Request listIndicesRequest = new Request("GET",
                    String.format(LIST_INDICES_ENDPOINT, ElasticsearchUtils.getTableIndexPattern()));

            List<IndexInfoResponse> indexInfoResponses = JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(listIndicesRequest)
                    .getEntity()
                    .getContent()), new TypeReference<List<IndexInfoResponse>>() {
            });

            return indexInfoResponses.stream()
                    .map(IndexInfoResponse::getIndex)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            log.error("Failed to fetch table indices list , error : ", e);
            throw new ElasticsearchQueryStoreException("Failed to fetch table indices list", e);
        }
    }

    @Override
    public List<ShardInfoResponse> getTableShardsInfo() {
        Request shardRequest = new Request("GET",
                String.format(SHARD_STATS_ENDPOINT, ElasticsearchUtils.getTableIndexPattern()));

        try {
            return JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(shardRequest)
                    .getEntity()
                    .getContent()), new TypeReference<List<ShardInfoResponse>>() {
            });
        } catch (Exception e) {
            log.error("Failed to fetch table shards info , error : ", e);
            throw new ElasticsearchQueryStoreException("Failed to fetch table shards info ", e);
        }
    }

    @Override
    public List<IndexInfoResponse> getTableIndicesInfo() {
        try {
            Request listIndicesRequest = new Request("GET",
                    String.format(LIST_INDICES_ENDPOINT, ElasticsearchUtils.getTableIndexPattern()));

            return JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(listIndicesRequest)
                    .getEntity()
                    .getContent()), new TypeReference<List<IndexInfoResponse>>() {
            });
        } catch (IOException e) {
            log.error("Failed to fetch table indices list , error : ", e);
            throw new ElasticsearchQueryStoreException("Failed to fetch table indices info", e);
        }
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
    @MonitoredFunction
    public void cleanup(Set<String> tables) {
        List<String> indicesToDelete = new ArrayList<>();
        try {
            GetIndexResponse response = connection.getClient()
                    .indices()
                    .get(new GetIndexRequest("*"), RequestOptions.DEFAULT);
            Set<String> currentIndices = Arrays.stream(response.getIndices())
                    .collect(Collectors.toSet());
            indicesToDelete = getIndicesToDelete(tables, currentIndices);
            deleteIndices(indicesToDelete);
        } catch (Exception e) {
            throw FoxtrotExceptions.createDataCleanupException(
                    String.format("Index Deletion Failed indexes - %s", indicesToDelete), e);
        }
    }

    @Override
    @SneakyThrows
    public ClusterHealthResponse getClusterHealth() {
        return connection.getClient()
                .cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
    }

    @Override
    public JsonNode getNodeStats() {
        try {
            return JsonUtils.toJsonNode(new InputStreamReader(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(new Request("GET", "/_nodes/stats"))
                    .getEntity()
                    .getContent()));
        } catch (IOException e) {
            log.error("Error finding node stats", e);
        }
        return JsonUtils.createObjectNode();
    }

    @Override
    public NodeFSStatsResponse getNodeFSStats() {
        Request nodeFSStatsRequest = new Request("GET", NODE_FS_STATS_ENDPOINT);

        try {
            return JsonUtils.fromJson(IOUtils.toByteArray(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(nodeFSStatsRequest)
                    .getEntity()
                    .getContent()), NodeFSStatsResponse.class);
        } catch (IOException e) {
            log.error("Failed to get node fs stats , error : ", e);
            throw new ElasticsearchQueryStoreException("Unable to get node fs stats");
        }
    }

    @Override
    public JsonNode getIndicesStats() {
        try {
            return JsonUtils.toJsonNode(new InputStreamReader(connection.getClient()
                    .getLowLevelClient()
                    .performRequest(new Request("GET", "/_stats"))
                    .getEntity()
                    .getContent()));
        } catch (IOException e) {
            log.error("Error finding indices stats", e);
        }
        return JsonUtils.createObjectNode();
    }

    @Override
    @MonitoredFunction
    public TableFieldMapping getFieldMappings(@MetricTerm String table) {
        return tableMetadataManager.getFieldMappings(table);
    }

    private Map<String, Object> convert(String table,
                                        Document document) {
        JsonNode metaNode = JsonUtils.objectToJsonNode(document.getMetadata());
        ObjectNode dataNode = document.getData()
                .deepCopy();
        dataNode.set(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME, metaNode);
        dataNode.set(ElasticsearchUtils.DOCUMENT_TIME_FIELD_NAME, JsonUtils.objectToJsonNode(document.getDate()));
        mutators.forEach(mutator -> mutator.mutate(table, document.getId(), dataNode));
        return JsonUtils.readMapFromObject(dataNode);
    }

    private void deleteIndices(List<String> indicesToDelete) {
        log.warn("Deleting Indexes - Indexes - {}", indicesToDelete);
        if (!indicesToDelete.isEmpty()) {
            List<List<String>> subLists = Lists.partition(indicesToDelete, 5);
            for (List<String> subList : subLists) {
                try {
                    connection.getClient()
                            .indices()
                            .delete(new DeleteIndexRequest(subList.toArray(new String[0])));
                    log.warn("Deleted Indexes - Indexes - {}", subList);
                } catch (Exception e) {
                    log.error("Index deletion failed - Indexes - {}", subList, e);
                }
            }
        }
    }

    private List<String> getIndicesToDelete(Set<String> tables,
                                            Collection<String> currentIndices) {
        List<String> indicesToDelete = new ArrayList<>();
        for (String currentIndex : currentIndices) {
            String table = ElasticsearchUtils.getTableNameFromIndex(currentIndex);
            if (table != null && tables.contains(table)) {
                boolean indexEligibleForDeletion;
                try {
                    indexEligibleForDeletion = ElasticsearchUtils.isIndexEligibleForDeletion(currentIndex,
                            tableMetadataManager.get(table));
                    if (indexEligibleForDeletion) {
                        log.warn("Index eligible for deletion : {}", currentIndex);
                        indicesToDelete.add(currentIndex);
                    }
                } catch (Exception ex) {
                    log.error("Unable to Get Table details for Table : {}", table, ex);
                }
            }
        }
        return indicesToDelete;
    }

    private void saveIndexMappingTemplate(final Table table) {
        try {
            PutIndexTemplateRequest templateRequest = templateMappingParser.buildIndexMappingTemplateRequest(table);
            connection.getClient()
                    .indices()
                    .putTemplate(templateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error occurred while initializing template for table : {}", table, e);
            throw FoxtrotExceptions.createTableInitializationException(table, e.getMessage());
        }
    }

}
