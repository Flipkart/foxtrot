package com.flipkart.foxtrot.core.pipeline.impl;

import com.flipkart.foxtrot.common.exception.PipelineMapStoreException;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.flipkart.foxtrot.pipeline.Pipeline;
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

public class PipelineMapStore implements MapStore<String, Pipeline>, Serializable {

    public static final String PIPELINE_META_INDEX = "pipeline-meta";
    public static final String PIPELINE_META_TYPE = "pipeline-meta";
    private static final Logger logger = LoggerFactory.getLogger(PipelineMapStore.class.getSimpleName());
    private static ElasticsearchConnection elasticsearchConnection;

    public PipelineMapStore(ElasticsearchConnection elasticsearchConnection) {
        PipelineMapStore.elasticsearchConnection = elasticsearchConnection;
    }

    public static PipelineMapStore.Factory factory(ElasticsearchConnection elasticsearchConnection) {
        return new PipelineMapStore.Factory(elasticsearchConnection);
    }

    @Override
    public void store(String key,
                      Pipeline value) {
        if (key == null || value == null || value.getName() == null) {
            throw new PipelineMapStoreException(String.format("Illegal Store Request - %s - %s", key, value));
        }
        logger.info("Storing key: {}", key);
        try {
            Map<String, Object> sourceMap = JsonUtils.readMapFromObject(value);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest().index(PIPELINE_META_INDEX)
                            .type(PIPELINE_META_TYPE)
                            .source(sourceMap)
                            .id(key)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new PipelineMapStoreException("Error saving Pipeline meta: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeAll(Map<String, Pipeline> map) {
        if (map == null) {
            throw new PipelineMapStoreException("Illegal Store Request - Null Map");
        }
        if (map.containsKey(null)) {
            throw new PipelineMapStoreException("Illegal Store Request - Null Key is Present");
        }

        logger.info("Store all called for multiple values");
        BulkRequest bulkRequestBuilder = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, Pipeline> mapEntry : map.entrySet()) {
            try {
                if (mapEntry.getValue() == null) {
                    throw new PipelineMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Pipeline - %s",
                                    mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = JsonUtils.readMapFromObject(mapEntry.getValue());
                bulkRequestBuilder.add(
                        new IndexRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, mapEntry.getKey()).source(sourceMap));
            } catch (Exception e) {
                throw new PipelineMapStoreException("Error Saving bulk pipelines: " + e.getMessage(), e);
            }
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PipelineMapStoreException("Error Saving bulk pipelines: " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String key) {
        logger.info("Delete called for value: {}", key);
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, key).setRefreshPolicy(
                            WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PipelineMapStoreException("Error deleting pipeline: " + e.getMessage(), e);
        }
        logger.info("Deleted value: {}", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.info("Delete all called for multiple values: {}", keys);
        BulkRequest bulRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String key : keys) {
            bulRequest.add(new DeleteRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, key));
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PipelineMapStoreException("Error deleting pipeline bulk: " + e.getMessage(), e);
        }
        logger.info("Deleted multiple values: {}", keys);
    }

    @Override
    public Pipeline load(String key) {
        logger.info("Load called for: {}", key);
        try {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(PIPELINE_META_INDEX, PIPELINE_META_TYPE, key), RequestOptions.DEFAULT);
            if (!response.isExists()) {
                return null;
            }
            return JsonUtils.fromJson(response.getSourceAsBytes(), Pipeline.class);
        } catch (Exception e) {
            logger.error("Error", e);
            throw new PipelineMapStoreException("Error getting data for pipeline: " + key);
        }
    }

    @Override
    public Map<String, Pipeline> loadAll(Collection<String> keys) {
        logger.info("Load all called for multiple keys");
        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        keys.forEach(key -> multiGetRequest.add(PIPELINE_META_INDEX, PIPELINE_META_TYPE, key));
        MultiGetResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PipelineMapStoreException("Error getting bulk pipelines: " + e.getMessage(), e);
        }
        Map<String, Pipeline> pipelines = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            try {
                Pipeline pipeline = JsonUtils.fromJson(multiGetItemResponse.getResponse()
                        .getSourceAsString(), Pipeline.class);
                pipelines.put(pipeline.getName(), pipeline);
            } catch (Exception e) {
                throw new PipelineMapStoreException(
                        String.format("Error getting data for pipeline [%s] : %s", multiGetItemResponse.getId(),
                                e.getMessage()), e);
            }
        }
        logger.info("Loaded value count: {}", pipelines.size());
        return pipelines;
    }

    @Override
    public Set<String> loadAllKeys() {
        logger.info("Load all keys called");
        SearchResponse response = null;
        try {
            response = elasticsearchConnection.getClient()
                    .search(new SearchRequest(PIPELINE_META_INDEX).types(PIPELINE_META_TYPE)
                            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                                    .size(ElasticsearchQueryUtils.QUERY_SIZE)
                                    .fetchSource(false))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PipelineMapStoreException("Error Loading pipeline Keys: " + e.getMessage(), e);
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
                throw new PipelineMapStoreException("Error Loading pipeline Keys: " + e.getMessage(), e);
            }
        } while (response.getHits()
                .getHits().length != 0);
        logger.info("Loaded value count: {}", ids.size());
        return ids;
    }


    public static class Factory implements MapStoreFactory<String, Pipeline>, Serializable {


        public Factory(ElasticsearchConnection elasticsearchConnection) {
            PipelineMapStore.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public PipelineMapStore newMapStore(String mapName,
                                            Properties properties) {
            return new PipelineMapStore(elasticsearchConnection);
        }
    }
}
