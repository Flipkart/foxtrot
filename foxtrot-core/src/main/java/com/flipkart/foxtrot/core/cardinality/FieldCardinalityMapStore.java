package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.exception.CardinalityMapStoreException;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FieldCardinalityMapStore implements MapStore<String, TableFieldMapping>, Serializable {

    public static final String CARDINALITY_CACHE_INDEX = "table_cardinality_cache";

    private static ElasticsearchConnection elasticsearchConnection;

    public FieldCardinalityMapStore(ElasticsearchConnection elasticsearchConnection) {
        FieldCardinalityMapStore.elasticsearchConnection = elasticsearchConnection;
    }

    public static FieldCardinalityMapStoreFactory factory(ElasticsearchConnection elasticsearchConnection) {
        return new FieldCardinalityMapStoreFactory(elasticsearchConnection);
    }

    @Override
    public void store(String table,
                      TableFieldMapping tableFieldMapping) {
        try {
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                            table).timeout(new TimeValue(2, TimeUnit.SECONDS))
                            .source(JsonUtils.toBytes(tableFieldMapping), XContentType.JSON), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("Error in saving cardinality cache for table:{} error message: {}", table, e.getMessage(), e);
            throw new CardinalityMapStoreException(
                    "Error in saving cardinality cache for table: " + table + " error message: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeAll(Map<String, TableFieldMapping> tableFieldMappingMap) {
        if (tableFieldMappingMap == null) {
            throw new CardinalityMapStoreException("Illegal Store Request - Null Map");
        }
        if (tableFieldMappingMap.containsKey(null)) {
            throw new CardinalityMapStoreException("Illegal Store Request - Null Key is Present");
        }

        log.info("Store all called for multiple values in field cardinality map");
        BulkRequest bulkRequestBuilder = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, TableFieldMapping> mapEntry : tableFieldMappingMap.entrySet()) {
            try {
                if (mapEntry.getValue() == null) {
                    throw new CardinalityMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Table - %s", mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = JsonUtils.readMapFromObject(mapEntry.getValue());
                bulkRequestBuilder.add(new IndexRequest(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                        mapEntry.getKey()).source(sourceMap));
            } catch (Exception e) {
                throw new CardinalityMapStoreException("Error bulk saving meta: ", e);
            }
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new CardinalityMapStoreException("Error while bulk saving field cardinality : ", e);
        }
    }

    @Override
    public void delete(String table) {
        log.info("Delete field cardinality called for table: {}", table);
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                            table).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new CardinalityMapStoreException("Error while deleting field cardinality ", e);
        }
        log.info("Deleted field cardinality for table: {}", table);
    }

    @Override
    public void deleteAll(Collection<String> tables) {
        log.info("Delete field cardinality called for tables: {}", tables);
        BulkRequest bulRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String table : tables) {
            bulRequest.add(new DeleteRequest(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME, table));
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new CardinalityMapStoreException("Error while bulk deleting field cardinality: ", e);
        }
        log.info("Deleted field cardinality for tables: {}", tables);
    }

    @Override
    public TableFieldMapping load(String table) {
        try {
            GetRequest getRequest = new GetRequest(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME,
                    table);
            GetResponse response = elasticsearchConnection.getClient()
                    .get(getRequest, RequestOptions.DEFAULT);
            if (!response.isExists() || response.isSourceEmpty()) {
                log.info("Could not find table field mapping data in elasticsearch for table : {}", table);
                return null;
            }
            return JsonUtils.fromJson(response.getSourceAsString(), TableFieldMapping.class);
        } catch (Exception e) {
            log.error("Error in getting cardinality cache for table: {} from elasticsearch, error message: {}", table,
                    e.getMessage(), e);
            throw new CardinalityMapStoreException(
                    "Error in getting cardinality cache for table: " + table + " from elasticsearch, error message: "
                            + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, TableFieldMapping> loadAll(Collection<String> tables) {
        Map<String, TableFieldMapping> tableFieldMappings = new HashMap<>();

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String table : tables) {
            multiGetRequest.add(CARDINALITY_CACHE_INDEX, ElasticsearchUtils.DOCUMENT_TYPE_NAME, table);
        }
        MultiGetResponse multiGetResponse;
        try {
            multiGetResponse = elasticsearchConnection.getClient()
                    .mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new CardinalityMapStoreException(
                    String.format("Failed to fetch table field mappings with tables : %s", tables));
        }

        for (MultiGetItemResponse itemResponse : multiGetResponse.getResponses()) {
            TableFieldMapping tableFieldMapping = JsonUtils.fromJson(itemResponse.getResponse()
                    .getSourceAsString(), TableFieldMapping.class);
            tableFieldMappings.put(itemResponse.getId(), tableFieldMapping);
            log.debug("Parsed and added table field mapping from elasticsearch table: {}, mapping: {}",
                    tableFieldMapping.getTable(), tableFieldMapping);
        }
        log.info("Loaded table field mappings in field cardinality cache with tables : {}", tables);
        return tableFieldMappings;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        try {
            List<String> tableList = new ArrayList<>();

            SearchRequest searchRequest = new SearchRequest(CARDINALITY_CACHE_INDEX);
            searchRequest.types(ElasticsearchUtils.DOCUMENT_TYPE_NAME);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(100);
            searchSourceBuilder.fetchSource(false);

            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(new TimeValue(60000));

            SearchResponse scrollResponse = elasticsearchConnection.getClient()
                    .search(searchRequest, RequestOptions.DEFAULT);

            do {
                for (SearchHit searchHit : scrollResponse.getHits()
                        .getHits()) {
                    tableList.add(searchHit.getId());
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollResponse.getScrollId());
                scrollRequest.scroll(TimeValue.timeValueSeconds(60000));
                scrollResponse = elasticsearchConnection.getClient()
                        .scroll(scrollRequest, RequestOptions.DEFAULT);
            } while (scrollResponse.getHits()
                    .getHits().length != 0);
            return tableList;
        } catch (IOException e) {
            throw new CardinalityMapStoreException("Failed to load all table keys in field cardinality map store");
        }
    }

    public static class FieldCardinalityMapStoreFactory implements MapStoreFactory<String, TableFieldMapping>,
            Serializable {

        public FieldCardinalityMapStoreFactory(ElasticsearchConnection elasticsearchConnection) {
            FieldCardinalityMapStore.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public FieldCardinalityMapStore newMapStore(String mapName,
                                                    Properties properties) {
            return new FieldCardinalityMapStore(elasticsearchConnection);
        }
    }
}
