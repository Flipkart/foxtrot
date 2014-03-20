package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:27 AM
 */
public class ElasticsearchQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryStore.class.getSimpleName());
    private static final String TYPE_NAME = "document";
    private static final String TABLENAME_PREFIX = "foxtrot";

    private ElasticsearchConnection connection;
    private DataStore dataStore;
    private ObjectMapper mapper;

    public ElasticsearchQueryStore(ElasticsearchConnection connection, DataStore dataStore, ObjectMapper mapper) {
        this.connection = connection;
        this.dataStore = dataStore;
        this.mapper = mapper;
    }

    @Override
    public void save(String table, Document document) throws QueryStoreException {
        try {
            dataStore.save(table, document);
            IndexResponse response = connection.getClient()
                                                .prepareIndex()
                                                .setIndex(getCurrentIndex(table))
                                                .setType(TYPE_NAME)
                                                .setId(document.getId())
                                                .setTimestamp(Long.toString(document.getTimestamp()))
                                                .setSource(mapper.writeValueAsBytes(document.getData()))
                                                .setRefresh(true)
                                                .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                                                .execute()
                                                .get();
            if(response.getVersion() > 0) {
                return;
            }
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR,
                            "Error saving documents: " + e.getMessage(), e);
        }
    }

    @Override
    public void save(String table, List<Document> documents) throws QueryStoreException {
        try {
            dataStore.save(table, documents);
            BulkRequestBuilder bulkRequestBuilder = connection.getClient().prepareBulk();
            final String index = getCurrentIndex(table);
            for(Document document: documents) {
                IndexRequest indexRequest = new IndexRequest()
                                                    .index(index)
                                                    .type(TYPE_NAME)
                                                    .id(document.getId())
                                                    .timestamp(Long.toString(document.getTimestamp()))
                                                    .source(mapper.writeValueAsBytes(document.getData()));
                bulkRequestBuilder.add(indexRequest);
            }

            BulkResponse response = bulkRequestBuilder.setRefresh(true)
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
    public List<Document> runQuery(final Query query) throws QueryStoreException {
        SearchRequestBuilder search = null;
        try {
            /*if(!tableManager.exists(query.getTable())) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "There is no table called: " + query.getTable());
            }*/
            search = connection.getClient().prepareSearch(getIndices(query.getTable()))
                    .setTypes(TYPE_NAME)
                    .setQuery(new ElasticSearchQueryGenerator(query.getCombiner()).genFilter(query.getFilters()))
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setFrom(query.getFrom())
                    .setSize(query.getLimit());
            if(null != query.getSort()) {
                search.addSort(query.getSort().getField(),
                        ResultSort.Order.desc == query.getSort().getOrder() ? SortOrder.DESC : SortOrder.ASC);
            }
	        logger.error("Running: " + search);		
            SearchResponse response = search.execute().actionGet();
            Vector<String> ids = new Vector<String>();
            for(SearchHit searchHit : response.getHits()) {
                ids.add(searchHit.getId());
            }
            if(ids.isEmpty()) {
                return Collections.emptyList();
            }
            return dataStore.get(query.getTable(), ids);
        } catch (Exception e) {
            if(null != search) {
                logger.error("Error running generated query: " + search);
            }
            else {
                logger.error("Query generation error: ", e);
            }
            try {
                throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                                "Error running query: " + mapper.writeValueAsString(query));
            } catch (JsonProcessingException e1) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_MALFORMED_QUERY_ERROR,
                                "Malformed query");
            }
        }

    }

    @Override
    public String runQueryAsync(Query query) throws QueryStoreException {
        final String id = UUID.randomUUID().toString();
        return null;
    }

    @Override
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException {
        return null;
    }

    //@Override
/*
    public JsonNode getDataForQuery(String table, String queryId) throws QueryStoreException {
        connection.getClient().prepareSearch().setIndices(getIndices(table));
        FilterBuilders.rangeFilter("xx").gt(1);
        ClusterStateResponse clusterStateResponse = connection.getClient().admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String, MappingMetaData> indexMappings = clusterStateResponse.getState().getMetaData().index(table).getMappings();
        try {
            byte data[] = mapper.writeValueAsBytes(indexMappings.get(TYPE_NAME).source().string());
            JsonSchema schema = mapper.generateJsonSchema(Document.class);

        } catch (Exception e) {
            e.printStackTrace();
        }
        */
/*
        1. Create and save json path:type mapping
        2. use path to find type
        3. Use type to convert query to proper filter

        * *//*

        return null;
    }
*/

    String[] getIndices(final String table) {
        /*long currentTime = new Date().getTime();
        String names[] = new String[30]; //TODO::USE TABLE METADATA
        for(int i = 0 ; i < 30; i++) {
            String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(currentTime));
            names[i] = String.format("%s-%s-%s", TABLENAME_PREFIX, table, postfix);
        }*/
        return new String[]{String.format("%s-%s-*", TABLENAME_PREFIX, table)};
    }

    String getCurrentIndex(final String table) {
        String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date());
        return String.format("%s-%s-%s", TABLENAME_PREFIX, table, postfix);
    }
}
