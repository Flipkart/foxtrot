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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.estimation.*;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.parsers.ElasticsearchMappingParser;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.IMap;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:11 PM
 */

public class DistributedTableMetadataManager implements TableMetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(DistributedTableMetadataManager.class);
    private static final String DATA_MAP = "tablemetadatamap";
    private static final String FIELD_MAP = "tablefieldmap";
    private static final String CARDINALITY_FIELD_MAP = "cardinalitytablefieldmap";
    private static final int PRECISION_THRESHOLD = 100;
    private static final int TIME_TO_LIVE_CACHE = (int) TimeUnit.MINUTES.toSeconds(15);
    private static final int TIME_TO_LIVE_TABLE_CACHE = (int) TimeUnit.DAYS.toSeconds(30);
    private static final int TIME_TO_LIVE_CARDINALITY_CACHE = (int) TimeUnit.DAYS.toSeconds(1);
    private static final int TIME_TO_NEAR_CACHE = (int) TimeUnit.MINUTES.toSeconds(15);
    private final HazelcastConnection hazelcastConnection;
    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper mapper;
    private IMap<String, Table> tableDataStore;
    private IMap<String, TableFieldMapping> fieldDataCache;
    private IMap<String, TableFieldMapping> fieldDataCardinalityCache;
    private final CardinalityConfig cardinalityConfig;

    public DistributedTableMetadataManager(HazelcastConnection hazelcastConnection,
                                           ElasticsearchConnection elasticsearchConnection,
                                           ObjectMapper mapper, CardinalityConfig cardinalityConfig) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;
        this.mapper = mapper;
        this.cardinalityConfig = cardinalityConfig;

        hazelcastConnection.getHazelcastConfig().getMapConfigs().put(DATA_MAP, tableMapConfig());
        hazelcastConnection.getHazelcastConfig().getMapConfigs().put(FIELD_MAP, fieldMetaMapConfig());
        hazelcastConnection.getHazelcastConfig().getMapConfigs().put(CARDINALITY_FIELD_MAP, cardinalityFieldMetaMapConfig());
    }


    private MapConfig tableMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_TABLE_CACHE);
        mapConfig.setBackupCount(0);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(TableMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_TABLE_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        return mapConfig;
    }

    private MapConfig fieldMetaMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CACHE);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_NEAR_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    private MapConfig cardinalityFieldMetaMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CARDINALITY_CACHE);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CARDINALITY_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    @Override
    public void save(Table table) throws FoxtrotException {
        logger.info(String.format("Saving Table : %s", table));
        tableDataStore.put(table.getName(), table);
        tableDataStore.flush();
    }

    @Override
    public Table get(String tableName) throws FoxtrotException {
        logger.debug(String.format("Getting Table : %s", tableName));
        if (tableDataStore.containsKey(tableName)) {
            return tableDataStore.get(tableName);
        }
        return null;
    }

    @Override
    @SneakyThrows
    public List<Table> get() throws FoxtrotException {
        if (0 == tableDataStore.size()) { //HACK::Check https://github.com/hazelcast/hazelcast/issues/1404
            return Collections.emptyList();
        }
        ArrayList<Table> tables = Lists.newArrayList(tableDataStore.values());
        tables.sort(Comparator.comparing(table -> table.getName().toLowerCase()));
        return tables;
    }

    @Override
    @Timed
    public TableFieldMapping getFieldMappings(String tableName, boolean withCardinality, boolean calculateCardinality) throws FoxtrotException {
        final String table = ElasticsearchUtils.getValidTableName(tableName);

        if (!tableDataStore.containsKey(table)) {
            throw FoxtrotExceptions.createBadRequestException(table,
                    String.format("unknown_table table:%s", table));
        }

        TableFieldMapping tableFieldMapping;
        if (fieldDataCache.containsKey(table) && !withCardinality) {
            tableFieldMapping = fieldDataCache.get(table);
        } else if (fieldDataCardinalityCache.containsKey(table) && withCardinality && !calculateCardinality) {
            tableFieldMapping = fieldDataCardinalityCache.get(table);
        } else {
            ElasticsearchMappingParser mappingParser = new ElasticsearchMappingParser(mapper);
            final String indices = ElasticsearchUtils.getIndices(table);
            logger.info("Selected indices: {}", indices);
            GetMappingsResponse mappingsResponse = elasticsearchConnection.getClient().admin()
                    .indices().prepareGetMappings(indices).execute().actionGet();

            Set<String> indicesName = Sets.newHashSet();
            for (ObjectCursor<String> index : mappingsResponse.getMappings().keys()) {
                indicesName.add(index.value);
            }
            List<FieldMetadata> fieldMetadata = indicesName.stream()
                    .filter(x -> !CollectionUtils.isNullOrEmpty(x))
                    .sorted((lhs, rhs) -> {
                        Date lhsDate = ElasticsearchUtils.parseIndexDate(lhs, table).toDate();
                        Date rhsDate = ElasticsearchUtils.parseIndexDate(rhs, table).toDate();
                        return 0 - lhsDate.compareTo(rhsDate);
                    })
                    .map(index -> mappingsResponse.mappings()
                            .get(index)
                            .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME))
                    .flatMap(mappingData -> {
                        try {
                            return mappingParser.getFieldMappings(mappingData).stream();
                        } catch (IOException e) {
                            logger.error("Could not read mapping from " + mappingData, e);
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toList());
            final TreeSet<FieldMetadata> fieldMetadataTreeSet = new TreeSet<>(new FieldMetadataComparator());
            fieldMetadataTreeSet.addAll(fieldMetadata);
            tableFieldMapping = new TableFieldMapping(table, fieldMetadataTreeSet);

            if (calculateCardinality) {
                estimateCardinality(table, tableFieldMapping.getMappings(), DateTime.now().minusDays(1).toDate().getTime());
                fieldDataCardinalityCache.put(table, tableFieldMapping);
            } else {
                fieldDataCache.put(table, tableFieldMapping);
            }
        }
        return TableFieldMapping.builder()
                .table(table)
                .mappings(tableFieldMapping.getMappings().stream()
                        .map(x -> FieldMetadata.builder()
                                .field(x.getField())
                                .type(x.getType())
                                .estimationData(withCardinality ? x.getEstimationData() : null)
                                .build())
                        .collect(Collectors.toSet()))
                .build();
    }

    @Override
    public void updateEstimationData(final String table, long timestamp) throws FoxtrotException {
        if (!tableDataStore.containsKey(table)) {
            throw FoxtrotExceptions.createBadRequestException(table,
                    String.format("unknown_table table:%s", table));
        }
        final TableFieldMapping tableFieldMapping = getFieldMappings(table, cardinalityConfig.isEnabled(), false);
        //estimateCardinality(table, tableFieldMapping.getMappings(), timestamp);
        fieldDataCache.put(table, tableFieldMapping);
    }

    private void estimateCardinality(final String table, final Collection<FieldMetadata> fields, long time) throws FoxtrotException {
        if (CollectionUtils.isNullOrEmpty(fields)) {
            logger.warn("No fields.. Nothing to query");
            return;
        }
        Map<String, FieldMetadata> fieldMap = fields.stream()
                .collect(Collectors.toMap(FieldMetadata::getField, fieldMetadata -> fieldMetadata, (lhs, rhs) -> lhs));

        final String index = ElasticsearchUtils.getCurrentIndex(ElasticsearchUtils.getValidTableName(table), time);
        final Client client = elasticsearchConnection.getClient();
        Map<String, EstimationData> estimationData = estimateFirstPhaseData(table, index, client, fieldMap);
        estimationData = estimateSecondPhaseData(table, index, client, estimationData);
        estimationData.forEach((key, value) -> fieldMap.get(key).setEstimationData(value));
    }

    private Map<String, EstimationData> estimateFirstPhaseData(String table,
                                                               String index,
                                                               Client client,
                                                               Map<String, FieldMetadata> fields) {
        MultiSearchRequestBuilder multiQuery = client.prepareMultiSearch();
        Map<String, EstimationData> estimationDataMap = Maps.newHashMap();
        int subListSize;
        if (cardinalityConfig == null || cardinalityConfig.getSubListSize() == 0) {
            subListSize = ElasticsearchUtils.DEFAULT_SUB_LIST_SIZE;
        } else {
            subListSize = cardinalityConfig.getSubListSize();
        }

        List<Map<String, FieldMetadata>> listofMaps =
                fields.entrySet().stream().collect(mapSize(subListSize));

        for (Map<String, FieldMetadata> innerMap : listofMaps) {

            innerMap.values().forEach(fieldMetadata -> {
                String field = fieldMetadata.getField();
                SearchRequestBuilder query = client.prepareSearch(index)
                        .setIndicesOptions(Utils.indicesOptions())
                        .setQuery(QueryBuilders.existsQuery(field))
                        .setSize(0);
                switch (fieldMetadata.getType()) {
                    case STRING: {
                        logger.info("table:{} field:{} type:{} aggregationType:{}", table, field, fieldMetadata.getType(), "cardinality");
                        query.addAggregation(AggregationBuilders.cardinality(field)
                                .field(field)
                                .precisionThreshold(PRECISION_THRESHOLD));
                        break;
                    }
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE: {
                        logger.info("table:{} field:{} type:{} aggregationType:{}", table, field, fieldMetadata.getType(), "percentile");
                        query.addAggregation(AggregationBuilders.percentiles(field)
                                .field(field)
                                .percentiles(10, 20, 30, 40, 50, 60, 70, 80, 90, 100));
                        query.addAggregation(AggregationBuilders.cardinality("_" + field)
                                .field(field)
                                .precisionThreshold(PRECISION_THRESHOLD));
                        break;
                    }
                    case BOOLEAN:
                    case DATE:
                    case OBJECT:
                }
                multiQuery.add(query);
            });
            Stopwatch stopwatch = Stopwatch.createStarted();
            MultiSearchResponse multiResponse;
            try {
                multiResponse = multiQuery.execute().actionGet();
            } finally {
                logger.info("Cardinality query on table {} for {} fields took {} ms",
                        table, fields.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }
            for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
                if (item.isFailure()) {
                    logger.info("FailureInDeducingCardinality table:{} failureMessage:{}", table,
                            item.getFailureMessage());
                    continue;
                }
                SearchResponse response = item.getResponse();
                final long hits = response.getHits().totalHits();
                Aggregations aggregations = response.getAggregations();
                if (null == aggregations) {
                    continue;
                }
                Map<String, Aggregation> output = aggregations.asMap();
                output.forEach((key, value) -> {
                    FieldMetadata fieldMetadata = fields.get(key);
                    if (fieldMetadata == null) {
                        fieldMetadata = fields.get(key.replace("_", ""));
                    }
                    if (fieldMetadata == null) {
                        return;
                    }
                    switch (fieldMetadata.getType()) {
                        case STRING: {
                            Cardinality cardinality = (Cardinality) value;
                            logger.info("table:{} field:{} type:{} aggregationType:{} value:{}",
                                    table, key, fieldMetadata.getType(), "cardinality", cardinality.getValue());
                            estimationDataMap.put(key, CardinalityEstimationData.builder()
                                    .cardinality(cardinality.getValue())
                                    .count(hits)
                                    .build());
                            break;
                        }
                        case INTEGER:
                        case LONG:
                        case FLOAT:
                        case DOUBLE: {
                            if (value instanceof Percentiles) {
                                Percentiles percentiles = (Percentiles) value;
                                double values[] = new double[10];
                                for (int i = 10; i <= 100; i += 10)
                                    values[(i / 10) - 1] = percentiles.percentile(i);
                                logger.info("table:{} field:{} type:{} aggregationType:{} value:{}",
                                        table, key, fieldMetadata.getType(), "percentile", values);
                                estimationDataMap.put(key, PercentileEstimationData.builder()
                                        .values(values)
                                        .count(hits)
                                        .build());
                            } else if (value instanceof Cardinality) {
                                Cardinality cardinality = (Cardinality) value;
                                logger.info("table:{} field:{} type:{} aggregationType:{} value:{}",
                                        table, key, fieldMetadata.getType(), "cardinality", cardinality.getValue());
                                EstimationData estimationData = estimationDataMap.get(key.replace("_", ""));
                                if (estimationData != null && estimationData instanceof PercentileEstimationData) {
                                    ((PercentileEstimationData) estimationData).setCardinality(cardinality.getValue());
                                } else {
                                    estimationDataMap.put(key.replace("_", ""), PercentileEstimationData.builder()
                                            .cardinality(cardinality.getValue())
                                            .build());
                                }
                            }
                            break;
                        }
                        case BOOLEAN: {
                            estimationDataMap.put(key, FixedEstimationData.builder()
                                    .count(2)
                                    .build());
                        }
                        case DATE:
                        case OBJECT:
                    }
                });
            }
        }
        return estimationDataMap;
    }

    private Map<String, EstimationData> estimateSecondPhaseData(String table,
                                                                String index,
                                                                Client client,
                                                                Map<String, EstimationData> estimationData) {
        long maxDocuments = estimationData.values().stream()
                .map(EstimationData::getCount).max(Comparator.naturalOrder()).orElse(0L);
        if (maxDocuments == 0) {
            return estimationData;
        }

        MultiSearchRequestBuilder multiQuery = client.prepareMultiSearch();
        estimationData.forEach((key, value) -> {
            value.accept(new EstimationDataVisitor<Void>() {
                @Override
                public Void visit(FixedEstimationData fixedEstimationData) {
                    return null;
                }

                @Override
                public Void visit(PercentileEstimationData percentileEstimationData) {
                    return null;
                }

                @Override
                public Void visit(CardinalityEstimationData cardinalityEstimationData) {
                    if (cardinalityEstimationData.getCount() > 0 && cardinalityEstimationData.getCardinality() > 0) {
                        int countToCardinalityRatio = (int) (cardinalityEstimationData.getCount() / cardinalityEstimationData.getCardinality());
                        int documentToCountRatio = (int) (maxDocuments / cardinalityEstimationData.getCount());
                        if (cardinalityEstimationData.getCardinality() <= 100
                                || (countToCardinalityRatio > 100 && documentToCountRatio < 100 && cardinalityEstimationData.getCardinality() <= 5000)) {
                            logger.info("field:{} maxCount:{} countToCardinalityRatio:{} documentToCountRatio:{}",
                                    key, maxDocuments, countToCardinalityRatio, documentToCountRatio);
                            SearchRequestBuilder query = client.prepareSearch(index)
                                    .setIndicesOptions(Utils.indicesOptions())
                                    .setQuery(QueryBuilders.existsQuery(key))
                                    .addAggregation(AggregationBuilders.terms(key).field(key).size(0))
                                    .setSize(0);
                            multiQuery.add(query);
                        }
                    }
                    return null;
                }

                @Override
                public Void visit(TermHistogramEstimationData termHistogramEstimationData) {
                    return null;
                }
            });
        });


        Map<String, EstimationData> estimationDataMap = Maps.newHashMap(estimationData);
        MultiSearchResponse multiResponse = multiQuery.execute().actionGet();
        for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
            if (item.isFailure()) {
                logger.info("FailureInDeducingCardinality table:{} failureMessage:{}", table,
                        item.getFailureMessage());
                continue;
            }
            SearchResponse response = item.getResponse();
            final long hits = response.getHits().totalHits();
            Aggregations aggregations = response.getAggregations();
            if (null == aggregations) {
                continue;
            }
            Map<String, Aggregation> output = aggregations.asMap();
            output.forEach((key, value) -> {
                        Terms terms = (Terms) output.get(key);
                        estimationDataMap.put(key, TermHistogramEstimationData.builder()
                                .count(hits)
                                .termCounts(terms.getBuckets().stream()
                                        .collect(Collectors.toMap(Terms.Bucket::getKeyAsString, Terms.Bucket::getDocCount)))
                                .build());
                    }
            );
        }
        return estimationDataMap;
    }

    @Override
    public boolean exists(String tableName) throws FoxtrotException {
        return tableDataStore.containsKey(tableName);
    }

    @Override
    public void delete(String tableName) throws FoxtrotException {
        logger.info(String.format("Deleting Table : %s", tableName));
        if (tableDataStore.containsKey(tableName)) {
            tableDataStore.delete(tableName);
        }
        logger.info(String.format("Deleted Table : %s", tableName));
    }

    @Override
    public void start() throws Exception {
        tableDataStore = hazelcastConnection.getHazelcast().getMap(DATA_MAP);
        fieldDataCache = hazelcastConnection.getHazelcast().getMap(FIELD_MAP);
        fieldDataCardinalityCache = hazelcastConnection.getHazelcast().getMap(CARDINALITY_FIELD_MAP);
    }

    @Override
    public void stop() throws Exception {
    }

    private static class FieldMetadataComparator implements Comparator<FieldMetadata>, Serializable {

        private static final long serialVersionUID = 8557746595191991528L;

        @Override
        public int compare(FieldMetadata o1, FieldMetadata o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            } else {
                return o1.getField().compareTo(o2.getField());
            }
        }
    }

    private static <K, V> Collector<Map.Entry<K, V>, ?, List<Map<K, V>>> mapSize(int limit) {
        return Collector.of(ArrayList::new,
                (l, e) -> {
                    if (l.isEmpty() || l.get(l.size() - 1).size() == limit) {
                        l.add(new HashMap<>());
                    }
                    l.get(l.size() - 1).put(e.getKey(), e.getValue());
                },
                (l1, l2) -> {
                    if (l1.isEmpty()) {
                        return l2;
                    }
                    if (l2.isEmpty()) {
                        return l1;
                    }
                    if (l1.get(l1.size() - 1).size() < limit) {
                        Map<K, V> map = l1.get(l1.size() - 1);
                        ListIterator<Map<K, V>> mapsIte = l2.listIterator(l2.size());
                        while (mapsIte.hasPrevious() && map.size() < limit) {
                            Iterator<Map.Entry<K, V>> ite = mapsIte.previous().entrySet().iterator();
                            while (ite.hasNext() && map.size() < limit) {
                                Map.Entry<K, V> entry = ite.next();
                                map.put(entry.getKey(), entry.getValue());
                                ite.remove();
                            }
                            if (!ite.hasNext()) {
                                mapsIte.remove();
                            }
                        }
                    }
                    l1.addAll(l2);
                    return l1;
                }
        );
    }
}
