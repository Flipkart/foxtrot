/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.indexmeta.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.exception.IndexMetadataStoreException;
import com.flipkart.foxtrot.common.exception.TableMapStoreException;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
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

import static com.flipkart.foxtrot.core.indexmeta.impl.IndexMetadataManagerImpl.INDEX_METADATA_DOCUMENT_TYPE;
import static com.flipkart.foxtrot.core.indexmeta.impl.IndexMetadataManagerImpl.INDEX_METADATA_INDEX;

public class IndexMetadataMapStore implements MapStore<String, TableIndexMetadata>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(IndexMetadataMapStore.class.getSimpleName());
    private static ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    public IndexMetadataMapStore(ElasticsearchConnection elasticsearchConnection) {
        IndexMetadataMapStore.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public static Factory factory(ElasticsearchConnection elasticsearchConnection) {
        return new Factory(elasticsearchConnection);
    }

    @Override
    public void store(String tableIndex,
                      TableIndexMetadata indexMetadata) {
        if (tableIndex == null || indexMetadata == null || indexMetadata.getIndexName() == null) {
            throw new IndexMetadataStoreException(
                    String.format("Illegal Store Request for table index metadata : index: %s, metadata:  %s",
                            tableIndex, indexMetadata));
        }
        logger.info("Storing metadata for table index: {}", tableIndex);
        try {
            Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, indexMetadata);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest().index(INDEX_METADATA_INDEX)
                            .type(INDEX_METADATA_DOCUMENT_TYPE)
                            .source(sourceMap)
                            .id(tableIndex)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new IndexMetadataStoreException("Error saving meta: ", e);
        }
    }

    @Override
    public void storeAll(Map<String, TableIndexMetadata> map) {
        if (map == null) {
            throw new IndexMetadataStoreException("Illegal Store Request - Null Map");
        }
        if (map.containsKey(null)) {
            throw new IndexMetadataStoreException("Illegal Store Request - Null Key is Present");
        }

        logger.info("Store all called for multiple values of index metadata");
        BulkRequest bulkRequestBuilder = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map.Entry<String, TableIndexMetadata> mapEntry : map.entrySet()) {
            try {
                if (mapEntry.getValue() == null) {
                    throw new TableMapStoreException(
                            String.format("Illegal Store Request - Object is Null for Table - %s", mapEntry.getKey()));
                }
                Map<String, Object> sourceMap = ElasticsearchQueryUtils.toMap(objectMapper, mapEntry.getValue());
                bulkRequestBuilder.add(
                        new IndexRequest(INDEX_METADATA_INDEX, INDEX_METADATA_DOCUMENT_TYPE, mapEntry.getKey()).source(
                                sourceMap));
            } catch (Exception e) {
                throw new IndexMetadataStoreException("Error bulk saving metadata for table indices: ", e);
            }
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException("Error saving metadata for all table indices: ", e);
        }
    }

    @Override
    public void delete(String tableIndex) {
        logger.info("Delete metadata called for table index: {}", tableIndex);
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(INDEX_METADATA_INDEX, INDEX_METADATA_DOCUMENT_TYPE,
                            tableIndex).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException(
                    String.format("Error deleting metadata for table indicex:%s ", tableIndex), e);
        }
        logger.info("Deleted metadata for table index: {}", tableIndex);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.info("Delete metadata called for multiple table indices: {}", keys);
        BulkRequest bulRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String key : keys) {
            bulRequest.add(new DeleteRequest(INDEX_METADATA_INDEX, INDEX_METADATA_DOCUMENT_TYPE, key));
        }
        try {
            elasticsearchConnection.getClient()
                    .bulk(bulRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException("Error bulk deleting metadata for table indices: ", e);
        }
        logger.info("Deleted metadata for table indices: {}", keys);
    }

    @Override
    public TableIndexMetadata load(String key) {
        logger.info("Load metadata called for table index: {}", key);
        try {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(INDEX_METADATA_INDEX, INDEX_METADATA_DOCUMENT_TYPE, key),
                            RequestOptions.DEFAULT);
            if (!response.isExists()) {
                return null;
            }
            return objectMapper.readValue(response.getSourceAsBytes(), TableIndexMetadata.class);
        } catch (Exception e) {
            logger.error("Error", e);
            throw new IndexMetadataStoreException("Error loading metadata for table index: " + key);
        }
    }

    @Override
    public Map<String, TableIndexMetadata> loadAll(Collection<String> keys) {
        logger.info("Load metadata called for multiple table indices : {}", keys);
        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        keys.forEach(key -> multiGetRequest.add(INDEX_METADATA_INDEX, INDEX_METADATA_DOCUMENT_TYPE, key));
        MultiGetResponse response;
        try {
            response = elasticsearchConnection.getClient()
                    .mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException("Error bulk saving metadata for table indices: ", e);
        }
        Map<String, TableIndexMetadata> tableIndicesMetadata = Maps.newHashMap();
        for (MultiGetItemResponse multiGetItemResponse : response) {
            if (!multiGetItemResponse.getResponse()
                    .isSourceEmpty()) {
                try {
                    TableIndexMetadata tableIndexMetadata = objectMapper.readValue(multiGetItemResponse.getResponse()
                            .getSourceAsString(), TableIndexMetadata.class);
                    tableIndicesMetadata.put(tableIndexMetadata.getIndexName(), tableIndexMetadata);
                } catch (Exception e) {
                    throw new IndexMetadataStoreException(
                            "Error getting metadata for table index: " + multiGetItemResponse.getId());
                }
            }
        }
        logger.info("Loaded metadata for table indices count: {}", tableIndicesMetadata.size());
        return tableIndicesMetadata;
    }

    @Override
    public Set<String> loadAllKeys() {
        logger.info("Load all table indices metadata keys called");
        SearchResponse response;
        try {
            response = elasticsearchConnection.getClient()
                    .search(new SearchRequest(INDEX_METADATA_INDEX).types(INDEX_METADATA_DOCUMENT_TYPE)
                            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                                    .size(ElasticsearchQueryUtils.QUERY_SIZE)
                                    .fetchSource(false))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException("Error reading saved metadata for table indices: ", e);
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
                throw new TableMapStoreException("Error reading saved metadata for table indices: ", e);
            }
        } while (response.getHits()
                .getHits().length != 0)
                ;
        logger.info("Loaded metadata for table indices count: {}", ids.size());
        return ids;
    }


    public static class Factory implements MapStoreFactory<String, TableIndexMetadata>, Serializable {

        public Factory(ElasticsearchConnection elasticsearchConnection) {
            IndexMetadataMapStore.elasticsearchConnection = elasticsearchConnection;
        }

        @Override
        public IndexMetadataMapStore newMapStore(String mapName,
                                                 Properties properties) {
            return new IndexMetadataMapStore(elasticsearchConnection);
        }
    }
}
