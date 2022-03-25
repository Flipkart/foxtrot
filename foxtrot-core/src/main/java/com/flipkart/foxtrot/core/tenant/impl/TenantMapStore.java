package com.flipkart.foxtrot.core.tenant.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.exception.TenantMapStoreException;
import com.flipkart.foxtrot.common.tenant.Tenant;
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

public class TenantMapStore implements MapStore<String, Tenant>, Serializable {

    public static final String TENANT_META_INDEX = "tenant-meta";
    private static final String TENANT_META_TYPE = "tenant-meta";
    private static final String ERROR_SAVING_BULK_META = "error saving bulk meta";
    private static final Logger logger = LoggerFactory.getLogger(TenantMapStore.class.getSimpleName());
    private static ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    public TenantMapStore(ElasticsearchConnection elasticsearchConnection) {
        TenantMapStore.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public static TenantMapStore.Factory factory(ElasticsearchConnection elasticsearchConnection) {
        return new TenantMapStore.Factory(elasticsearchConnection);
    }

    @Override
    public void store(String key,
                      Tenant value) {
        if (key == null || value == null || value.getTenantName() == null) {
            throw new TenantMapStoreException(String.format("Illegal Store Request - %s - %s", key, value));
        }
        logger.info("Storing key: {}", key);
        try {
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, value);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest().index(TENANT_META_INDEX)
                            .type(TENANT_META_TYPE)
                            .source(sourceMap)
                            .id(key)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new TenantMapStoreException("Error saving Tenant meta: ", e);
        }
    }

    @Override
    public void storeAll(Map<String, Tenant> map) {
        if (map == null) {
            throw new TenantMapStoreException("Illegal Store Request - Null Map");
        }
        if (map.containsKey(null)) {
            throw new TenantMapStoreException("Illegal Store Request - Null Key is Present");
        }

        logger.info("Store all called for multiple values");
        BulkRequest bulkRequestBuilder = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, Tenant> mapEntry : map.entrySet()) {
            try {
                if (mapEntry.getValue() == null) {
                    throw new TenantMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Tenant - %s", mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, mapEntry.getValue());
                bulkRequestBuilder.add(
                        new IndexRequest(TENANT_META_INDEX, TENANT_META_TYPE, mapEntry.getKey()).source(sourceMap));
            } catch (Exception e) {
                throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
            }
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
        }
    }

    @Override
    public void delete(String key) {
        logger.info("Delete called for value: {}", key);
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(TENANT_META_INDEX, TENANT_META_TYPE, key).setRefreshPolicy(
                            WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
        }
        logger.info("Deleted value: {}", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.info("Delete all called for multiple values: {}", keys);
        BulkRequest bulRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String key : keys) {
            bulRequest.add(new DeleteRequest(TENANT_META_INDEX, TENANT_META_TYPE, key));
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
        }
        logger.info("Deleted multiple values: {}", keys);
    }

    @Override
    public Tenant load(String key) {
        logger.info("Load called for: {}", key);
        try {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(TENANT_META_INDEX, TENANT_META_TYPE, key), RequestOptions.DEFAULT);
            if (!response.isExists()) {
                return null;
            }
            return objectMapper.readValue(response.getSourceAsBytes(), Tenant.class);
        } catch (Exception e) {
            logger.error("Error", e);
            throw new TenantMapStoreException("Error getting data for tenant: " + key);
        }
    }

    @Override
    public Map<String, Tenant> loadAll(Collection<String> keys) {
        logger.info("Load all called for multiple keys");
        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        keys.forEach(key -> multiGetRequest.add(TENANT_META_INDEX, TENANT_META_TYPE, key));
        MultiGetResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
        }
        Map<String, Tenant> tenants = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            try {
                Tenant tenant = objectMapper.readValue(multiGetItemResponse.getResponse()
                        .getSourceAsString(), Tenant.class);
                tenants.put(tenant.getTenantName(), tenant);
            } catch (Exception e) {
                throw new TenantMapStoreException("Error getting data for tenant: " + multiGetItemResponse.getId());
            }
        }
        logger.info("Loaded value count: {}", tenants.size());
        return tenants;
    }

    @Override
    public Set<String> loadAllKeys() {
        logger.info("Load all keys called");
        SearchResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .search(new SearchRequest(TENANT_META_INDEX).types(TENANT_META_TYPE)
                            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                                    .size(ElasticsearchQueryUtils.QUERY_SIZE)
                                    .fetchSource(false))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
        }
        Set<String> ids = Sets.newHashSet();
        do {
            for (SearchHit hit : response.getHits()
                    .getHits()) {
                ids.add(hit.getId());
            }
            if (0 == response.getHits()
                    .getHits().length) {
                break;
            }
            try {
                response = elasticsearchConnection.getClient()
                        .scroll(new SearchScrollRequest(response.getScrollId()).scroll(new TimeValue(60000)),
                                RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new TenantMapStoreException(ERROR_SAVING_BULK_META + ": ", e);
            }
        } while (response.getHits()
                .getHits().length != 0)
                ;
        logger.info("Loaded value count: {}", ids.size());
        return ids;
    }


    public static class Factory implements MapStoreFactory<String, Tenant>, Serializable {

        public Factory(ElasticsearchConnection elasticsearchConnection) {
            TenantMapStore.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public TenantMapStore newMapStore(String mapName,
                                          Properties properties) {
            return new TenantMapStore(elasticsearchConnection);
        }
    }
}
