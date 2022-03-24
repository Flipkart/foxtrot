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

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.exception.IndexMetadataStoreException;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadataAttributes;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.Lists;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.common.exception.ErrorCode.EXECUTION_EXCEPTION;

@Slf4j
@Singleton
public class IndexMetadataManagerImpl implements IndexMetadataManager {

    public static final String INDEX_METADATA_INDEX = "table-index-metadata";
    public static final String INDEX_METADATA_DOCUMENT_TYPE = "table-index-metadata";
    private static final String INDEX_METADATA_MAP = "indexmetadatamap";
    private static final int TIME_TO_LIVE_INDEX_METADATA_CACHE = (int) TimeUnit.DAYS.toSeconds(7);


    private final HazelcastConnection hazelcastConnection;
    private final ElasticsearchConnection elasticsearchConnection;
    private IMap<String, TableIndexMetadata> indexMetadataStore;

    @Inject
    public IndexMetadataManagerImpl(final HazelcastConnection hazelcastConnection,
                                    final ElasticsearchConnection elasticsearchConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(indexMetadataMapConfig());
    }

    private MapConfig indexMetadataMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(INDEX_METADATA_MAP);
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_INDEX_METADATA_CACHE);
        mapConfig.setBackupCount(0);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(IndexMetadataMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_INDEX_METADATA_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        return mapConfig;
    }

    public void save(TableIndexMetadata tableIndexMetadata) {
        indexMetadataStore.put(tableIndexMetadata.getIndexName(), tableIndexMetadata);
        indexMetadataStore.flush();
    }

    public void update(String indexName,
                       TableIndexMetadata tableIndexMetadata) {
        indexMetadataStore.put(indexName, tableIndexMetadata);
        indexMetadataStore.flush();
    }

    @Override
    public List<TableIndexMetadata> getAll() {
        if (0 == indexMetadataStore.size()) { //HACK::Check https://github.com/hazelcast/hazelcast/issues/1404
            return Collections.emptyList();
        }
        List<TableIndexMetadata> tableIndicesMetadata = Lists.newArrayList(indexMetadataStore.values());
        tableIndicesMetadata.sort(Comparator.comparing(TableIndexMetadata::getIndexName));
        return tableIndicesMetadata;
    }

    public TableIndexMetadata getByIndex(String indexName) {
        if (indexMetadataStore.containsKey(indexName)) {
            return indexMetadataStore.get(indexName);
        }
        return null;
    }

    @Override
    public List<TableIndexMetadata> getByTable(String table) {
        List<TableIndexMetadata> tableIndicesMetadata = new ArrayList<>();
        QueryBuilder query = new TermQueryBuilder(TableIndexMetadataAttributes.TABLE, table);
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX_METADATA_INDEX).types(INDEX_METADATA_DOCUMENT_TYPE)
                    .source(new SearchSourceBuilder().query(query)
                            .fetchSource(true)
                            .size(ElasticsearchQueryUtils.QUERY_SIZE))
                    .searchType(SearchType.QUERY_THEN_FETCH);
            SearchHits response = elasticsearchConnection.getClient()
                    .search(searchRequest, RequestOptions.DEFAULT)
                    .getHits();
            if (response == null || response.getTotalHits() == 0) {
                return Lists.newArrayList();
            }
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(response.getHits())) {
                tableIndicesMetadata.add(JsonUtils.fromJson(searchHit.getSourceAsString(), TableIndexMetadata.class));
            }
            return tableIndicesMetadata;
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel get by funnel id failed", e);
        }
    }

    @Override
    public List<TableIndexMetadata> search(List<Filter> filters) {
        List<TableIndexMetadata> tableIndicesMetadata = new ArrayList<>();
        try {
            SearchResponse searchResponse = elasticsearchConnection.getClient()
                    .search(new SearchRequest(INDEX_METADATA_INDEX).types(INDEX_METADATA_DOCUMENT_TYPE)
                            .source(new SearchSourceBuilder().query(
                                    new ElasticSearchQueryGenerator().genFilter(filters))
                                    .size(ElasticsearchQueryUtils.QUERY_SIZE)
                                    .fetchSource(true))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
            do {
                for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchResponse.getHits()
                        .getHits())) {
                    tableIndicesMetadata.add(
                            JsonUtils.fromJson(searchHit.getSourceAsString(), TableIndexMetadata.class));
                }

                if (0 == searchResponse.getHits()
                        .getHits().length) {
                    break;
                }

                searchResponse = getScrollResponse(filters, searchResponse);
            } while (searchResponse.getHits()
                    .getHits().length != 0);
            return tableIndicesMetadata;
        } catch (Exception e) {
            throw new IndexMetadataStoreException("Index metadata search failed", e);
        }
    }

    private SearchResponse getScrollResponse(List<Filter> filters,
                                             SearchResponse searchResponse) {
        try {
            searchResponse = elasticsearchConnection.getClient()
                    .scroll(new SearchScrollRequest(searchResponse.getScrollId()).scroll(new TimeValue(60000)),
                            RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IndexMetadataStoreException(
                    String.format("Error while searching index metadata for filters: %s ", filters), e);
        }
        return searchResponse;
    }

    public void delete(String indexName) {
        log.info("Deleting Metadata for Table Index : {}", indexName);
        if (indexMetadataStore.containsKey(indexName)) {
            indexMetadataStore.delete(indexName);
        }
        log.info("Deleted Metadata for Table Index : {}", indexName);
    }

    @Override
    public void start() throws Exception {
        indexMetadataStore = hazelcastConnection.getHazelcast()
                .getMap(INDEX_METADATA_MAP);
    }

    @Override
    public void stop() throws Exception {
        // do nothing
    }
}
