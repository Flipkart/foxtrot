package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    private final QueryExecutor queryExecutor;


    public ElasticsearchQueryStore(TableMetadataManager tableMetadataManager,
                                   ElasticsearchConnection connection,
                                   DataStore dataStore, QueryExecutor queryExecutor) {
        this.tableMetadataManager = tableMetadataManager;
        this.connection = connection;
        this.dataStore = dataStore;
        this.mapper = ElasticsearchUtils.getMapper();
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void save(String table, Document document) throws QueryStoreException {
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            dataStore.save(table, document);
            long timestamp = document.getTimestamp();
            connection.getClient()
                    .prepareIndex()
                    .setIndex(ElasticsearchUtils.getCurrentIndex(table, timestamp))
                    .setType(ElasticsearchUtils.TYPE_NAME)
                    .setId(document.getId())
                    .setTimestamp(Long.toString(timestamp))
                    .setSource(mapper.writeValueAsBytes(document.getData()))
                    .setRefresh(true)
                    .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                    .execute()
                    .get();
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                    "Error saving documents: " + e.getMessage(), e);
        }
    }

    @Override
    public void save(String table, List<Document> documents) throws QueryStoreException {
        try {
            if (!tableMetadataManager.exists(table)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "No table exists with the name: " + table);
            }
            dataStore.save(table, documents);
            BulkRequestBuilder bulkRequestBuilder = connection.getClient().prepareBulk();
            for (Document document : documents) {
                long timestamp = document.getTimestamp();
                final String index = ElasticsearchUtils.getCurrentIndex(table, timestamp);
                IndexRequest indexRequest = new IndexRequest()
                        .index(index)
                        .type(ElasticsearchUtils.TYPE_NAME)
                        .id(document.getId())
                        .timestamp(Long.toString(timestamp))
                        .source(mapper.writeValueAsBytes(document.getData()));
                bulkRequestBuilder.add(indexRequest);
            }
            bulkRequestBuilder.setRefresh(true)
                    .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                    .execute()
                    .get();

        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                    "Error saving documents: " + e.getMessage(), e);
        }
    }

    @Override
    public Document get(String table, String id) throws QueryStoreException {
        try {
            return dataStore.get(table, id);
        } catch (DataStoreException e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    "Error getting documents: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Document> get(String table, List<String> ids) throws QueryStoreException {
        try {
            return dataStore.get(table, ids);
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR,
                    "Error getting documents: " + e.getMessage(), e);
        }
    }

    @Override
    public ActionResponse runQuery(final Query query) throws QueryStoreException {
        return queryExecutor.execute(query);
    }

    @Override
    public AsyncDataToken runQueryAsync(Query query) throws QueryStoreException {
        return queryExecutor.executeAsync(query);
    }

    @Override
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException {
        return null;
    }
}
