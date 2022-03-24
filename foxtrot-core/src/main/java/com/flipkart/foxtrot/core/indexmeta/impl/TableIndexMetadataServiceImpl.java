package com.flipkart.foxtrot.core.indexmeta.impl;

import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.util.Utils;
import com.flipkart.foxtrot.core.indexmeta.IndexMetadataManager;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.StorageSizeUtils;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.parseIndexDate;
import static com.flipkart.foxtrot.core.rebalance.ShardInfoResponse.ShardType.PRIMARY;

@Slf4j
@Singleton
public class TableIndexMetadataServiceImpl implements TableIndexMetadataService {

    private final TableMetadataManager tableMetadataManager;
    private final IndexMetadataManager indexMetadataManager;
    private final QueryStore queryStore;

    @Inject
    public TableIndexMetadataServiceImpl(final TableMetadataManager tableMetadataManager,
                                         final IndexMetadataManager indexMetadataManager,
                                         final QueryStore queryStore) {
        this.tableMetadataManager = tableMetadataManager;
        this.indexMetadataManager = indexMetadataManager;
        this.queryStore = queryStore;
    }

    @Override
    public void syncTableIndexMetadata(int oldMetadataSyncDays) {

        DateTime currentDateTime = new DateTime();

        DateTime endTime = new DateTime().minusDays(1).withTime(23, 59, 59, 999);

        DateTime syncOldMetadataTillDateTime = endTime.minusDays(oldMetadataSyncDays - 1).withTime(0, 0, 0, 0);

        List<IndexInfoResponse> tableIndicesInfo = queryStore.getTableIndicesInfo();

        List<ShardInfoResponse> tableShardsInfo = queryStore.getTableShardsInfo();

        Map<String, Integer> indexVsShardCount = new HashMap<>();
        tableShardsInfo.stream()
                .filter(tableShardInfo -> PRIMARY.getName()
                        .equals(tableShardInfo.getPrimaryOrReplica()))
                .forEach(tableShardInfo -> indexVsShardCount.put(tableShardInfo.getIndex(),
                        indexVsShardCount.getOrDefault(tableShardInfo.getIndex(), 0) + 1));

        log.info("Table indices info :{}", tableIndicesInfo);
        log.info("Shard count for indices :{}", indexVsShardCount);

        tableIndicesInfo.stream()
                .filter(indexInfoResponse -> {
                    DateTime indexDateTime = parseIndexDate(indexInfoResponse.getIndex());
                    boolean eligibleForMetadataSync = indexDateTime.toDate()
                            .compareTo(syncOldMetadataTillDateTime.toDate()) >= 0;
                    log.info("Index {} {} eligible for metadata sync, sync old metadata for previous {} days",
                            indexInfoResponse.getIndex(), eligibleForMetadataSync
                                    ? ""
                                    : "not", oldMetadataSyncDays);
                    return eligibleForMetadataSync;
                })
                .map(indexInfoResponse -> {
                    try {
                        String table = ElasticsearchUtils.getTableNameFromIndex(indexInfoResponse.getIndex());
                        long noOfColumns = tableMetadataManager.getColumnCount(table);
                        long shardCount = indexVsShardCount.getOrDefault(indexInfoResponse.getIndex(), 0);
                        long averageShardSizeInBytes = shardCount > 0
                                ? indexInfoResponse.getPrimaryStoreSize() / shardCount
                                : 0;
                        long averageEventSizeInBytes = indexInfoResponse.getDocCount() > 0
                                ? indexInfoResponse.getPrimaryStoreSize()
                                / indexInfoResponse.getDocCount()
                                : 0;
                        DateTime indexDate = parseIndexDate(indexInfoResponse.getIndex());
                        return TableIndexMetadata.builder()
                                .indexName(indexInfoResponse.getIndex())
                                .table(ElasticsearchUtils.getTableNameFromIndex(indexInfoResponse.getIndex()))
                                .datePostFix(ElasticsearchUtils.getIndexDatePostfix(indexInfoResponse.getIndex()))
                                .table(table)
                                .noOfEvents(indexInfoResponse.getDocCount())
                                .shardCount(shardCount)
                                .noOfColumns(noOfColumns)
                                .totalIndexSizeInBytes(indexInfoResponse.getPrimaryStoreSize())
                                .totalIndexSizeInMBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToMegaBytes(indexInfoResponse.getPrimaryStoreSize()), 2))
                                .totalIndexSizeInTBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToTeraBytes(indexInfoResponse.getPrimaryStoreSize()), 2))
                                .totalIndexSizeInGBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToGigaBytes(indexInfoResponse.getPrimaryStoreSize()), 2))
                                .averageEventSizeInBytes(averageEventSizeInBytes)
                                .averageEventSizeInKBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToKiloBytes(averageEventSizeInBytes), 2))
                                .averageEventSizeInMBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToMegaBytes(averageEventSizeInBytes), 2))
                                .averageShardSizeInBytes(averageShardSizeInBytes)
                                .averageShardSizeInGBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToGigaBytes(averageShardSizeInBytes), 2))
                                .averageShardSizeInMBs(Utils.convertToDecimalPlaces(
                                        StorageSizeUtils.bytesToMegaBytes(averageShardSizeInBytes), 2))
                                .timestamp(indexDate.getMillis())
                                .date(new Date(indexDate))
                                .updatedAt(currentDateTime.getMillis())
                                .build();
                    } catch (Exception e) {
                        log.error("Error while calculating table index metadata for index : {}",
                                indexInfoResponse.getIndex(), e);
                        return null;
                    }

                })
                .filter(Objects::nonNull)
                .forEach(tableIndexMetadata -> {
                    try {
                        indexMetadataManager.save(tableIndexMetadata);
                    } catch (Exception e) {
                        log.error("Error while saving table index metadata for index :{}",
                                tableIndexMetadata.getIndexName(), e);
                    }

                });
    }

    @Override
    public TableIndexMetadata getIndexMetadata(String indexName) {
        return indexMetadataManager.getByIndex(indexName);
    }

    @Override
    public void cleanupIndexMetadata(int retentionDays) {
        List<Table> tables = tableMetadataManager.get();

        tables.stream()
                .map(Table::getName)
                .forEach(table -> {
                    List<TableIndexMetadata> tableIndicesMetadata = indexMetadataManager.getByTable(table);
                    if (!CollectionUtils.isNullOrEmpty(tableIndicesMetadata)) {
                        tableIndicesMetadata.stream()
                                .filter(tableIndexMetadata -> isMetadataEligibleForDeletion(
                                        tableIndexMetadata.getIndexName(), retentionDays))
                                .forEach(tableIndexMetadata -> {
                                    try {
                                        indexMetadataManager.delete(tableIndexMetadata.getIndexName());
                                        log.info(
                                                "Deleted index metadata for index: {}, found eligible for deletion with retention days: {}",
                                                tableIndexMetadata.getIndexName(), retentionDays);
                                    } catch (Exception e) {
                                        log.error("Error while deleting eligible index metadata for index : {}",
                                                tableIndexMetadata.getIndexName(), e);
                                    }
                                });
                    }
                });
    }

    @Override
    public List<TableIndexMetadata> getAllIndicesMetadata() {
        return indexMetadataManager.getAll();
    }

    @Override
    public List<TableIndexMetadata> searchIndexMetadata(List<Filter> filters) {
        return indexMetadataManager.search(filters);
    }

    @Override
    public List<TableIndexMetadata> getTableIndicesMetadata(String table) {
        return indexMetadataManager.getByTable(table);
    }

    private boolean isMetadataEligibleForDeletion(String index,
                                                  int retentionDays) {
        DateTime indexDate = parseIndexDate(index);
        DateTime endTime = new DateTime();
        DateTime startTime = endTime.minusDays(retentionDays + 1)
                .toDateTime();
        return indexDate.toDate()
                .compareTo(startTime.toDate()) < 0;
    }
}
