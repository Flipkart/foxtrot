package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TableMapStore implements MapStore<String, Table> {
    private static final Logger logger = LoggerFactory.getLogger(TableMapStore.class.getSimpleName());

    public static final String TABLE_META_INDEX = "table-meta";
    public static final String TABLE_META_TYPE = "table-meta";

    public static class Factory implements MapStoreFactory<String, Table> {
        private final ElasticsearchConnection elasticsearchConnection;

        public Factory(ElasticsearchConnection elasticsearchConnection) {
            this.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public TableMapStore newMapStore(String mapName, Properties properties) {
            return new TableMapStore(elasticsearchConnection);
        }
    }

    public static Factory factory(ElasticsearchConnection elasticsearchConnection) {
        return new Factory(elasticsearchConnection);
    }

    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    public TableMapStore(ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    @Override
    public void store(String key, Table value) {
        try {
            elasticsearchConnection.getClient().prepareIndex()
                    .setIndex(TABLE_META_INDEX)
                    .setType(TABLE_META_TYPE)
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .setSource(objectMapper.writeValueAsString(value))
                    .setId(key)
                    .setRefresh(true)
                    .execute()
                    .actionGet();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error saving meta: ", e);
        }
    }

    @Override
    public void storeAll(Map<String, Table> map) {
        BulkRequestBuilder bulRequestBuilder = elasticsearchConnection.getClient().prepareBulk().setConsistencyLevel(WriteConsistencyLevel.ALL).setRefresh(true);
        for (Map.Entry<String, Table> mapEntry : map.entrySet()) {
            try {
                bulRequestBuilder.add(elasticsearchConnection.getClient()
                        .prepareIndex(TABLE_META_INDEX, TABLE_META_TYPE, mapEntry.getKey())
                        .setSource(objectMapper.writeValueAsString(mapEntry.getValue())));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error bulk saving meta: ", e);
            }
        }
        bulRequestBuilder.execute().actionGet();
    }

    @Override
    public void delete(String key) {
        elasticsearchConnection.getClient().prepareDelete()
                .setIndex(TABLE_META_INDEX)
                .setType(TABLE_META_TYPE)
                .setId(key)
                .execute()
                .actionGet();
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        BulkRequestBuilder bulRequestBuilder = elasticsearchConnection.getClient().prepareBulk().setConsistencyLevel(WriteConsistencyLevel.ALL).setRefresh(true);
        for (String key : keys) {
            bulRequestBuilder.add(elasticsearchConnection.getClient()
                    .prepareDelete(TABLE_META_INDEX, TABLE_META_TYPE, key));
        }
        bulRequestBuilder.execute().actionGet();
    }

    @Override
    public Table load(String key) {
        GetResponse response = elasticsearchConnection.getClient().prepareGet()
                .setIndex(TABLE_META_INDEX)
                .setType(TABLE_META_TYPE)
                .setId(key)
                .execute()
                .actionGet();
        if (!response.isExists()) {
            return null;
        }
        try {
            return objectMapper.readValue(response.getSourceAsBytes(), Table.class);
        } catch (IOException e) {
            throw new RuntimeException("Error getting data for table: " + key);
        }
    }

    @Override
    public Map<String, Table> loadAll(Collection<String> keys) {
        MultiGetResponse response = elasticsearchConnection.getClient()
                .prepareMultiGet()
                .add(TABLE_META_INDEX, TABLE_META_TYPE, keys)
                .execute()
                .actionGet();
        Map<String, Table> tables = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            try {
                Table table = objectMapper.readValue(multiGetItemResponse.getResponse().getSourceAsString(),
                        Table.class);
                tables.put(table.getName(), table);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }

    @Override
    public Set<String> loadAllKeys() {
        SearchResponse response = elasticsearchConnection.getClient()
                .prepareSearch(TABLE_META_INDEX)
                .setTypes(TABLE_META_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(30, TimeUnit.SECONDS))
                .setNoFields()
                .execute()
                .actionGet();
        Set<String> ids = Sets.newHashSet();
        while (true) {
            response = elasticsearchConnection.getClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }
            if (0 == response.getHits().hits().length) {
                break;
            }
        }

        return ids;
    }
}
