/*
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
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.exception.CardinalityCalculationException;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationResult;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationService;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.cardinality.FieldCardinalityMapStore;
import com.flipkart.foxtrot.core.parsers.ElasticsearchMappingParser;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.RequestOptions;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:11 PM
 */
@Singleton
@Order(15)
@Slf4j
public class DistributedTableMetadataManager implements TableMetadataManager {

    private static final String DATA_MAP = "tablemetadatamap";
    private static final String FIELD_MAP = "tablefieldmap";
    private static final String CARDINALITY_FIELD_MAP = "cardinalitytablefieldmap";
    private static final String CARDINALITY = "cardinality";
    private static final int PRECISION_THRESHOLD = 100;
    private static final int TIME_TO_LIVE_CACHE = (int) TimeUnit.MINUTES.toSeconds(15);
    private static final int TIME_TO_LIVE_TABLE_CACHE = (int) TimeUnit.DAYS.toSeconds(30);
    private static final int TIME_TO_LIVE_CARDINALITY_CACHE = (int) TimeUnit.DAYS.toSeconds(7);
    private static final int TIME_TO_NEAR_CACHE = (int) TimeUnit.MINUTES.toSeconds(15);
    private final HazelcastConnection hazelcastConnection;
    private final ElasticsearchConnection elasticsearchConnection;
    private final CardinalityConfig cardinalityConfig;
    private final CardinalityCalculationService cardinalityCalculationService;
    private IMap<String, Table> tableDataStore;
    private IMap<String, TableFieldMapping> fieldDataCache;
    private IMap<String, TableFieldMapping> fieldDataCardinalityCache;

    @Inject
    public DistributedTableMetadataManager(HazelcastConnection hazelcastConnection,
                                           ElasticsearchConnection elasticsearchConnection,
                                           CardinalityCalculationService cardinalityCalculationService,
                                           CardinalityConfig cardinalityConfig) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;
        this.cardinalityCalculationService = cardinalityCalculationService;
        this.cardinalityConfig = cardinalityConfig;

        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(tableMapConfig());
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(fieldMetaMapConfig());
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(cardinalityFieldMetaMapConfig());
    }

    private MapConfig tableMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(DATA_MAP);
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
        mapConfig.setName(FIELD_MAP);
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
        mapConfig.setName(CARDINALITY_FIELD_MAP);
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CARDINALITY_CACHE);
        mapConfig.setBackupCount(0);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(FieldCardinalityMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CARDINALITY_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    @Override
    @MonitoredFunction
    public void save(Table table) {
        tableDataStore.put(table.getName(), table);
        tableDataStore.flush();
    }

    @Override
    @MonitoredFunction
    public Table get(String tableName) {
        if (tableDataStore.containsKey(tableName)) {
            return tableDataStore.get(tableName);
        }
        return null;
    }

    @Override
    @SneakyThrows
    public List<Table> get() {
        if (0 == tableDataStore.size()) { //HACK::Check https://github.com/hazelcast/hazelcast/issues/1404
            return Collections.emptyList();
        }
        ArrayList<Table> tables = Lists.newArrayList(tableDataStore.values());
        tables.sort(Comparator.comparing(table -> table.getName()
                .toLowerCase()));
        return tables;
    }

    @Override
    @Timed
    @MonitoredFunction
    public CardinalityCalculationResult calculateCardinality(String table) {
        TableFieldMapping tableFieldMapping;

        if (!tableDataStore.containsKey(table)) {
            log.error("Error while calculating cardinality: Table :{} is not present", table);
            throw FoxtrotExceptions.createBadRequestException(table, unknownTableMessage(table));
        }

        tableFieldMapping = getTableFieldMapping(table);
        CardinalityCalculationResult cardinalityCalculationResult = cardinalityCalculationService.calculateCardinality(
                tableFieldMapping);

        TableFieldMapping oldTableFieldMapping = fieldDataCardinalityCache.get(table);

        TableFieldMapping mergedFieldMapping = mergeFieldMappings(oldTableFieldMapping,
                cardinalityCalculationResult.getTableFieldMapping());

        fieldDataCardinalityCache.put(table, mergedFieldMapping);

        return cardinalityCalculationResult;


    }

    private String unknownTableMessage(String table) {
        return String.format("unknown_table table:%s", table);
    }

    /*
        If estimation data is found in old table field mapping and we have a newer estimation data, then replace it
        If estimation data is found in old table field mapping but there's no estimation data in new calculation, then retain it
        If there's new estimation data for a new field from new calculation, then add it
     */
    private TableFieldMapping mergeFieldMappings(TableFieldMapping oldTableFieldMapping,
                                                 TableFieldMapping newTableFieldMapping) {

        if (oldTableFieldMapping == null) {
            return newTableFieldMapping;
        }

        Map<String, FieldMetadata> oldFieldMappings = oldTableFieldMapping.getMappings()
                .stream()
                .collect(Collectors.toMap(FieldMetadata::getField, Function.identity()));
        Map<String, FieldMetadata> newFieldMappings = newTableFieldMapping.getMappings()
                .stream()
                .collect(Collectors.toMap(FieldMetadata::getField, Function.identity()));

        newFieldMappings.forEach((key, value) -> {
            if (!oldFieldMappings.containsKey(key) || (oldFieldMappings.containsKey(key) && (oldFieldMappings.get(key)
                    .getEstimationData() == null || value.getEstimationData() != null))) {
                oldFieldMappings.put(key, value);
            }
        });

        return TableFieldMapping.builder()
                .table(newTableFieldMapping.getTable())
                .mappings(new HashSet<>(oldFieldMappings.values()))
                .build();
    }


    @Override
    public TableFieldMapping getFieldMappings(String tableName) {
        final String table = ElasticsearchUtils.getValidName(tableName);

        if (!tableDataStore.containsKey(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, unknownTableMessage(table));
        }

        TableFieldMapping tableFieldMapping;
        if (fieldDataCache.containsKey(table)) {
            tableFieldMapping = fieldDataCache.get(table);
        } else {
            tableFieldMapping = getTableFieldMapping(table);
            fieldDataCache.put(table, tableFieldMapping);
        }
        return TableFieldMapping.builder()
                .table(table)
                .mappings(tableFieldMapping.getMappings()
                        .stream()
                        .map(x -> FieldMetadata.builder()
                                .field(x.getField())
                                .type(x.getType())
                                .build())
                        .collect(Collectors.toSet()))
                .build();
    }

    @Override
    public long getColumnCount(String table) {
        TableFieldMapping fieldMappings = getFieldMappings(table);
        return fieldMappings != null && fieldMappings.getMappings() != null
                ? fieldMappings.getMappings()
                .size()
                : 0L;
    }

    @Override
    public TableFieldMapping getFieldMappingsWithCardinality(String tableName) {
        final String table = ElasticsearchUtils.getValidName(tableName);

        if (!tableDataStore.containsKey(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, unknownTableMessage(table));
        }

        TableFieldMapping tableFieldMapping = fieldDataCardinalityCache.get(table);

        return TableFieldMapping.builder()
                .table(table)
                .mappings(tableFieldMapping != null
                        ? tableFieldMapping.getMappings()
                        .stream()
                        .map(x -> FieldMetadata.builder()
                                .field(x.getField())
                                .type(x.getType())
                                .estimationData(x.getEstimationData())
                                .build())
                        .collect(Collectors.toSet())
                        : Sets.newHashSet())
                .build();
    }

    @Override
    public void updateEstimationData(final String table,
                                     long timestamp) {
        if (!tableDataStore.containsKey(table)) {
            throw FoxtrotExceptions.createBadRequestException(table, unknownTableMessage(table));
        }
        final TableFieldMapping tableFieldMapping = cardinalityConfig.isEnabled()
                ? getFieldMappingsWithCardinality(table)
                : getFieldMappings(table);
        fieldDataCache.put(table, tableFieldMapping);
    }

    @Override
    public boolean exists(String tableName) {
        return tableDataStore.containsKey(tableName);
    }

    @Override
    public void delete(String tableName) {
        log.info("Deleting Table : {}", tableName);
        if (tableDataStore.containsKey(tableName)) {
            tableDataStore.delete(tableName);
        }
        log.info("Deleted Table : {}", tableName);
    }

    @Override
    public void start() {
        tableDataStore = hazelcastConnection.getHazelcast()
                .getMap(DATA_MAP);
        fieldDataCache = hazelcastConnection.getHazelcast()
                .getMap(FIELD_MAP);
        fieldDataCardinalityCache = hazelcastConnection.getHazelcast()
                .getMap(CARDINALITY_FIELD_MAP);
    }

    @Override
    public void stop() {
        //do nothing
    }

    private TableFieldMapping getTableFieldMapping(String table) {
        ElasticsearchMappingParser mappingParser = new ElasticsearchMappingParser();
        final String indices = ElasticsearchUtils.getIndices(table);
        log.info("Fetching table field mapping for indices: {}", indices);
        final GetMappingsResponse mappingsResponse;
        try {
            mappingsResponse = elasticsearchConnection.getClient()
                    .indices()
                    .getMapping(new GetMappingsRequest().indices(indices), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error while fetching field mappings for table:{} indices :{}", table, indices, e);
            throw new CardinalityCalculationException("Error fetching field mappings for table: " + table, e);
        }

        Set<String> indicesName = Sets.newHashSet();
        log.debug("Fetched mapping response for indices: {}, response: {}", indices, mappingsResponse);
        for (ObjectCursor<String> index : mappingsResponse.getMappings()
                .keys()) {
            indicesName.add(index.value);
        }
        List<FieldMetadata> fieldMetadata = indicesName.stream()
                .filter(x -> !CollectionUtils.isNullOrEmpty(x))
                .sorted((lhs, rhs) -> {
                    Date lhsDate = ElasticsearchUtils.parseIndexDate(lhs, table)
                            .toDate();
                    Date rhsDate = ElasticsearchUtils.parseIndexDate(rhs, table)
                            .toDate();
                    return rhsDate.compareTo(lhsDate);
                })
                .map(index -> mappingsResponse.mappings()
                        .get(index)
                        .get(ElasticsearchUtils.DOCUMENT_TYPE_NAME))
                .flatMap(mappingData -> {
                    try {
                        return mappingParser.getFieldMappings(mappingData)
                                .stream();
                    } catch (Exception e) {
                        log.error("Error for table :{} while parsing mapping data into field mapping from :{}", table,
                                mappingData, e);
                        throw new CardinalityCalculationException(
                                "Error for table " + table + " while parsing mapping data into field mapping from "
                                        + mappingData, e);
                    }
                })
                .collect(Collectors.toList());
        final TreeSet<FieldMetadata> fieldMetadataTreeSet = new TreeSet<>(new FieldMetadataComparator());
        fieldMetadataTreeSet.addAll(fieldMetadata);
        return new TableFieldMapping(table, fieldMetadataTreeSet, new Date());
    }

    private static class FieldMetadataComparator implements Comparator<FieldMetadata>, Serializable {

        private static final long serialVersionUID = 8557746595191991528L;

        @Override
        public int compare(FieldMetadata o1,
                           FieldMetadata o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            } else {
                return o1.getField()
                        .compareTo(o2.getField());
            }
        }
    }
}
