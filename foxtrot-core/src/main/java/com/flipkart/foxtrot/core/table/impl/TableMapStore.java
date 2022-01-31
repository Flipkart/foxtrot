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
package com.flipkart.foxtrot.core.table.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.exception.TableMapStoreException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TableMapStore implements MapStore<String, Table>, Serializable {
    public static final String TABLE_META_INDEX = "table-meta";
    public static final String TABLE_META_TYPE = "table-meta";
    private static final Logger logger = LoggerFactory.getLogger(TableMapStore.class.getSimpleName());
    private static ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    public TableMapStore(ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public static Factory factory(ElasticsearchConnection elasticsearchConnection) {
        return new Factory(elasticsearchConnection);
    }

    @Override
    public void store(String key, Table value) {
        if(key == null || value == null || value.getName() == null) {
            throw new TableMapStoreException(String.format("Illegal Store Request - %s - %s", key, value));
        }
        logger.info("Storing key: {}", key);
        try {
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, value);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest()
                            .index(TABLE_META_INDEX)
                            .type(TABLE_META_TYPE)
                            .source(sourceMap)
                            .id(key)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new TableMapStoreException("Error saving meta: ", e);
        }
    }

    @Override
    public void storeAll(Map<String, Table> map) {
        if(map == null) {
            throw new TableMapStoreException("Illegal Store Request - Null Map");
        }
        if(map.containsKey(null)) {
            throw new TableMapStoreException("Illegal Store Request - Null Key is Present");
        }

        logger.info("Store all called for multiple values");
        BulkRequest bulkRequestBuilder = new BulkRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for(Map.Entry<String, Table> mapEntry : map.entrySet()) {
            try {
                if(mapEntry.getValue() == null) {
                    throw new TableMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Table - %s", mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, mapEntry.getValue());
                bulkRequestBuilder.add(new IndexRequest(TABLE_META_INDEX, TABLE_META_TYPE, mapEntry.getKey())
                                               .source(sourceMap));
            } catch (Exception e) {
                throw new TableMapStoreException("Error bulk saving meta: ", e);
            }
        }
        try {
            elasticsearchConnection
                    .getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableMapStoreException("Error bulk saving meta: ", e);
        }
    }

    @Override
    public void delete(String key) {
        logger.info("Delete called for value: {}", key);
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(TABLE_META_INDEX, TABLE_META_TYPE, key)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableMapStoreException("Error bulk saving meta: ", e);
        }
        logger.info("Deleted value: {}", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.info("Delete all called for multiple values: {}", keys);
        BulkRequest bulRequest = new BulkRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for(String key : keys) {
            bulRequest.add(new DeleteRequest(TABLE_META_INDEX, TABLE_META_TYPE, key));
        }
        try {
            elasticsearchConnection
                    .getClient()
                    .bulk(bulRequest, RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableMapStoreException("Error bulk saving meta: ", e);
        }
        logger.info("Deleted multiple values: {}", keys);
    }

    @Override
    public Table load(String key) {
        logger.info("Load called for: {}", key);
        try {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TABLE_META_INDEX, TABLE_META_TYPE, key), RequestOptions.DEFAULT);
            if(!response.isExists()) {
                return null;
            }
            return objectMapper.readValue(response.getSourceAsBytes(), Table.class);
        } catch (Exception e) {
            logger.error("Error", e);
            throw new TableMapStoreException("Error getting data for table: " + key);
        }
    }

    @Override
    public Map<String, Table> loadAll(Collection<String> keys) {
        logger.info("Load all called for multiple keys");
        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        keys.forEach(key -> multiGetRequest.add(TABLE_META_INDEX, TABLE_META_TYPE, key));
        MultiGetResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .multiGet(multiGetRequest, RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableMapStoreException("Error bulk saving meta: ", e);
        }
        Map<String, Table> tables = Maps.newHashMap();
        for(MultiGetItemResponse multiGetItemResponse : response) {
            try {
                Table table = objectMapper.readValue(multiGetItemResponse.getResponse()
                                                             .getSourceAsString(), Table.class);
                tables.put(table.getName(), table);
            } catch (Exception e) {
                throw new TableMapStoreException("Error getting data for table: " + multiGetItemResponse.getId());
            }
        }
        logger.info("Loaded value count: {}", tables.size());
        return tables;
    }

    @Override
    public Set<String> loadAllKeys() {
        logger.info("Load all keys called");
        SearchResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .search(new SearchRequest(TABLE_META_INDEX)
                            .types(TABLE_META_TYPE)
                            .source(new SearchSourceBuilder()
                                            .query(QueryBuilders.matchAllQuery())
                                            .size(ElasticsearchQueryUtils.QUERY_SIZE)
                                   .fetchSource(false))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
        }
        catch (IOException e) {
            throw new TableMapStoreException("Error bulk saving meta: ", e);
        }
        Set<String> ids = Sets.newHashSet();
        do {
            for(SearchHit hit : response.getHits()
                    .getHits()) {
                ids.add(hit.getId());
            }
            if(0 == response.getHits()
                    .getHits().length) {
                break;
            }
            try {
                response = elasticsearchConnection.getClient()
                        .scroll(new SearchScrollRequest(response.getScrollId())
                                        .scroll(new TimeValue(60000)), RequestOptions.DEFAULT);
            }
            catch (IOException e) {
                throw new TableMapStoreException("Error bulk saving meta: ", e);
            }
        } while (response.getHits()
                         .getHits().length != 0)
                ;
        logger.info("Loaded value count: {}", ids.size());
        return ids;
    }

    public static class Factory implements MapStoreFactory<String, Table>, Serializable {

        public Factory(ElasticsearchConnection elasticsearchConnection) {
            TableMapStore.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public TableMapStore newMapStore(String mapName, Properties properties) {
            return new TableMapStore(elasticsearchConnection);
        }
    }
}

