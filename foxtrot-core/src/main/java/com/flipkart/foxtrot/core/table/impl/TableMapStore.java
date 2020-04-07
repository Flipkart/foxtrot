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
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ElasticsearchConnection elasticsearchConnection;
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
                    .prepareIndex()
                    .setIndex(TABLE_META_INDEX)
                    .setType(TABLE_META_TYPE)
                    .setSource(sourceMap)
                    .setId(key)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute()
                    .actionGet();
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
        BulkRequestBuilder bulkRequestBuilder = elasticsearchConnection.getClient()
                .prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for(Map.Entry<String, Table> mapEntry : map.entrySet()) {
            try {
                if(mapEntry.getValue() == null) {
                    throw new TableMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Table - %s", mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, mapEntry.getValue());
                bulkRequestBuilder.add(elasticsearchConnection.getClient()
                                               .prepareIndex(TABLE_META_INDEX, TABLE_META_TYPE, mapEntry.getKey())
                                               .setSource(sourceMap));
            } catch (Exception e) {
                throw new TableMapStoreException("Error bulk saving meta: ", e);
            }
        }
        bulkRequestBuilder.execute()
                .actionGet();
    }

    @Override
    public void delete(String key) {
        logger.info("Delete called for value: {}", key);
        elasticsearchConnection.getClient()
                .prepareDelete()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setIndex(TABLE_META_INDEX)
                .setType(TABLE_META_TYPE)
                .setId(key)
                .execute()
                .actionGet();
        logger.info("Deleted value: {}", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.info("Delete all called for multiple values: {}", keys);
        BulkRequestBuilder bulRequestBuilder = elasticsearchConnection.getClient()
                .prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for(String key : keys) {
            bulRequestBuilder.add(elasticsearchConnection.getClient()
                                          .prepareDelete(TABLE_META_INDEX, TABLE_META_TYPE, key));
        }
        bulRequestBuilder.execute()
                .actionGet();
        logger.info("Deleted multiple values: {}", keys);
    }

    @Override
    public Table load(String key) {
        logger.info("Load called for: {}", key);
        GetResponse response = elasticsearchConnection.getClient()
                .prepareGet()
                .setIndex(TABLE_META_INDEX)
                .setType(TABLE_META_TYPE)
                .setId(key)
                .execute()
                .actionGet();
        if(!response.isExists()) {
            return null;
        }
        try {
            return objectMapper.readValue(response.getSourceAsBytes(), Table.class);
        } catch (Exception e) {
            throw new TableMapStoreException("Error getting data for table: " + key);
        }
    }

    @Override
    public Map<String, Table> loadAll(Collection<String> keys) {
        logger.info("Load all called for multiple keys");
        MultiGetResponse response = elasticsearchConnection.getClient()
                .prepareMultiGet()
                .add(TABLE_META_INDEX, TABLE_META_TYPE, keys)
                .execute()
                .actionGet();
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
        SearchResponse response = elasticsearchConnection.getClient()
                .prepareSearch(TABLE_META_INDEX)
                .setTypes(TABLE_META_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(ElasticsearchQueryUtils.QUERY_SIZE)
                .setScroll(new TimeValue(30, TimeUnit.SECONDS))
                .setFetchSource(false)
                .execute()
                .actionGet();
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
            response = elasticsearchConnection.getClient()
                    .prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        } while (response.getHits()
                         .getHits().length != 0)
                ;
        logger.info("Loaded value count: {}", ids.size());
        return ids;
    }

    public static class Factory implements MapStoreFactory<String, Table>, Serializable {

        private final ElasticsearchConnection elasticsearchConnection;

        public Factory(ElasticsearchConnection elasticsearchConnection) {
            this.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public TableMapStore newMapStore(String mapName, Properties properties) {
            return new TableMapStore(elasticsearchConnection);
        }
    }
}
