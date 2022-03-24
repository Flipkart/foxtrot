package com.flipkart.foxtrot.core.shardtuning;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroupDetailResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.events.model.shardtuning.*;
import com.flipkart.foxtrot.core.events.model.shardtuning.ShardCountTuningEvent.NodeGroupMetadata;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadataAttributes;
import com.flipkart.foxtrot.core.nodegroup.NodeGroupManager;
import com.flipkart.foxtrot.core.table.TableManager;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class ShardCountTuningServiceImpl implements ShardCountTuningService {

    private final ShardCountTuningJobConfig shardCountTuningJobConfig;
    private final TableIndexMetadataService tableIndexMetadataService;
    private final TableManager tableManager;
    private final NodeGroupManager nodeGroupManager;
    private final EventBusManager eventBusManager;

    @Inject
    public ShardCountTuningServiceImpl(final ShardCountTuningJobConfig shardCountTuningJobConfig,
                                       final TableIndexMetadataService tableIndexMetadataService,
                                       final TableManager tableManager,
                                       final EventBusManager eventBusManager,
                                       final NodeGroupManager nodeGroupManager) {
        this.shardCountTuningJobConfig = shardCountTuningJobConfig;
        this.tableIndexMetadataService = tableIndexMetadataService;
        this.tableManager = tableManager;
        this.nodeGroupManager = nodeGroupManager;
        this.eventBusManager = eventBusManager;
    }

    public void tuneShardCount() {
        List<Table> tables = tableManager.getAll();
        tables.forEach(this::tuneShardCount);
        eventBusManager.postEvent(buildShardTuningJobFinishEvent().toIngestionEvent());
    }

    public void tuneShardCount(String tableName) {
        Table table = tableManager.get(tableName);
        tuneShardCount(table);
    }

    private void tuneShardCount(Table table) {
        DateTime endTime = new DateTime().minusDays(1)
                .withTime(23, 59, 59, 999);

        DateTime startTime = endTime.minusDays(shardCountTuningJobConfig.getRollingWindowInDays() - 1)
                .withTime(0, 0, 0, 0);

        try {
            log.info("Tuning shard count for table:{} considering metadata for duration start: {}, duration end: {}",
                    table.getName(), startTime, endTime);
            List<TableIndexMetadata> tableIndicesMetadata = getTableIndicesMetadata(table, startTime, endTime);

            log.info("Tuning shard count for table:{} with indices metadata: {}", table.getName(),
                    tableIndicesMetadata);

            Map<String, Double> dateVsIndexSize = tableIndicesMetadata.stream()
                    .collect(Collectors.toMap(TableIndexMetadata::getDatePostFix,
                            TableIndexMetadata::getTotalIndexSizeInGBs));

            double maxIndexSizeInGBs = dateVsIndexSize.values()
                    .stream()
                    .mapToDouble(i -> i)
                    .max()
                    .orElse(0.0);

            int idealShardCount = (int) Math.ceil(
                    maxIndexSizeInGBs / shardCountTuningJobConfig.getIdealShardSizeInGBs());
            idealShardCount = idealShardCount > 0 ? idealShardCount : 1;

            AllocatedESNodeGroup allocatedESNodeGroup = nodeGroupManager.getNodeGroupByTable(table.getName());

            if (Objects.nonNull(allocatedESNodeGroup)) {
                tuneShardCount(table, dateVsIndexSize, maxIndexSizeInGBs, idealShardCount, allocatedESNodeGroup);
            } else {
                log.info(
                        "Skip tuning shard count for table: {} with node group allocated: none, ideal shard count: {}, max index size in GBs : {}, ideal shard size: {}",
                        table.getName(), idealShardCount, maxIndexSizeInGBs,
                        shardCountTuningJobConfig.getIdealShardSizeInGBs());
                eventBusManager.postEvent(
                        buildShardTuningSkipEvent(shardCountTuningJobConfig, dateVsIndexSize, table.getName(),
                                maxIndexSizeInGBs, idealShardCount).toIngestionEvent());
            }
        } catch (Exception e) {
            log.error("Error while tuning shard count for table : {}", table.getName(), e);
            eventBusManager.postEvent(
                    buildShardTuningFailureEvent(table.getName(), shardCountTuningJobConfig).toIngestionEvent());
        }
    }

    private void tuneShardCount(Table table,
                                Map<String, Double> dateVsIndexSize,
                                double maxIndexSizeInGBs,
                                int idealShardCount,
                                AllocatedESNodeGroup allocatedESNodeGroup) {
        ESNodeGroupDetailResponse nodeGroupDetails = nodeGroupManager.getNodeGroupDetails(
                allocatedESNodeGroup.getGroupName());
        int maxShardCapacity = (nodeGroupDetails.getDetails()
                .getNodeCount() * allocatedESNodeGroup.getTableAllocation()
                .getTotalShardsPerNode()) / shardCountTuningJobConfig.getReplicationFactor();
        if (maxShardCapacity >= idealShardCount) {
            log.info("Tuning shard count for table :{} to {}, max index size in GB for last {} days : {}",
                    table.getName(), idealShardCount, shardCountTuningJobConfig.getRollingWindowInDays(),
                    maxIndexSizeInGBs);
            eventBusManager.postEvent(
                    buildShardTuningAuditEvent(table.getName(), dateVsIndexSize, maxIndexSizeInGBs, idealShardCount,
                            allocatedESNodeGroup.getGroupName(), maxShardCapacity).toIngestionEvent());
            updateShardCount(table, dateVsIndexSize, maxIndexSizeInGBs, allocatedESNodeGroup.getGroupName(),
                    maxShardCapacity, idealShardCount);

        } else {
            log.info("Can not tune shard count for table :{}. Ideal shard Count {} is more than Max Shard Capacity {} "
                            + "for node group : {}, node count: {}, total shards per node: {}. So setting shard count to max capacity",
                    table.getName(), idealShardCount, maxShardCapacity, allocatedESNodeGroup.getGroupName(),
                    nodeGroupDetails.getDetails()
                            .getNodeCount(), allocatedESNodeGroup.getTableAllocation()
                            .getTotalShardsPerNode());
            eventBusManager.postEvent(
                    buildShardTuningAuditEvent(table.getName(), dateVsIndexSize, maxIndexSizeInGBs, maxShardCapacity,
                            allocatedESNodeGroup.getGroupName(), maxShardCapacity).toIngestionEvent());
            updateShardCount(table, dateVsIndexSize, maxIndexSizeInGBs, allocatedESNodeGroup.getGroupName(),
                    maxShardCapacity, maxShardCapacity);

        }
    }

    private void updateShardCount(Table table,
                                  Map<String, Double> dateVsIndexSize,
                                  double maxIndexSizeInGBs,
                                  String nodeGroup,
                                  int maxShardCapacity,
                                  int idealShardCount) {
        if (shardCountTuningJobConfig.isShardCountTuningEnabled()) {
            table.setShards(idealShardCount);
            tableManager.update(table);
            eventBusManager.postEvent(
                    buildShardTuningSuccessEvent(table.getName(), dateVsIndexSize, maxIndexSizeInGBs, idealShardCount,
                            nodeGroup, maxShardCapacity).toIngestionEvent());
        }
    }

    private List<TableIndexMetadata> getTableIndicesMetadata(Table table,
                                                             DateTime startTime,
                                                             DateTime endTime) {
        List<Filter> filters = new ArrayList<>();

        filters.add(EqualsFilter.builder()
                .field(TableIndexMetadataAttributes.TABLE)
                .value(table.getName())
                .build());

        filters.add(BetweenFilter.builder()
                .field(TableIndexMetadataAttributes.TIMESTAMP)
                .from(startTime.toDate()
                        .getTime())
                .to(endTime.toDate()
                        .getTime())
                .build());
        return tableIndexMetadataService.searchIndexMetadata(filters);
    }

    private ShardTuningJobFinishEvent buildShardTuningJobFinishEvent() {
        return ShardTuningJobFinishEvent.builder()
                .build();
    }

    private ShardCountTuningSkipEvent buildShardTuningSkipEvent(ShardCountTuningJobConfig shardCountTuningJobConfig,
                                                                Map<String, Double> dateVsIndexSize,
                                                                String table,
                                                                double maxIndexSizeInGBs,
                                                                int idealShardCount) {
        return ShardCountTuningSkipEvent.builder()
                .idealShardSizeInGBs(shardCountTuningJobConfig.getIdealShardSizeInGBs())
                .maxIndexSizeInGBs(maxIndexSizeInGBs)
                .rollingWindowInDays(shardCountTuningJobConfig.getRollingWindowInDays())
                .table(table)
                .dateVsIndexSize(JsonUtils.toJson(dateVsIndexSize))
                .idealShardCount(idealShardCount)
                .build();
    }

    private ShardCountTuningAuditEvent buildShardTuningAuditEvent(String table,
                                                                  Map<String, Double> dateVsIndexSize,
                                                                  double maxIndexSizeInGBs,
                                                                  int idealShardCount,
                                                                  String nodeGroup,
                                                                  int maxShardCapacity) {
        return ShardCountTuningAuditEvent.builder()
                .dateVsIndexSize(JsonUtils.toJson(dateVsIndexSize))
                .idealShardCount(idealShardCount)
                .nodeGroupMetadata(NodeGroupMetadata.builder()
                        .maxShardCapacity(maxShardCapacity)
                        .nodeGroup(nodeGroup)
                        .build())
                .shardCountTuningJobConfig(shardCountTuningJobConfig)
                .maxIndexSizeInGBs(maxIndexSizeInGBs)
                .table(table)
                .build();
    }


    private ShardCountTuningSuccessEvent buildShardTuningSuccessEvent(String table,
                                                                      Map<String, Double> dateVsIndexSize,
                                                                      double maxIndexSizeInGBs,
                                                                      int idealShardCount,
                                                                      String nodeGroup,
                                                                      int maxShardCapacity) {
        return ShardCountTuningSuccessEvent.builder()
                .dateVsIndexSize(JsonUtils.toJson(dateVsIndexSize))
                .idealShardCount(idealShardCount)
                .nodeGroupMetadata(NodeGroupMetadata.builder()
                        .maxShardCapacity(maxShardCapacity)
                        .nodeGroup(nodeGroup)
                        .build())
                .shardCountTuningJobConfig(shardCountTuningJobConfig)
                .maxIndexSizeInGBs(maxIndexSizeInGBs)
                .table(table)
                .build();
    }

    private ShardCountTuningFailureEvent buildShardTuningFailureEvent(String table,
                                                                      ShardCountTuningJobConfig shardCountTuningJobConfig) {
        return ShardCountTuningFailureEvent.builder()
                .rollingWindowInDays(shardCountTuningJobConfig.getRollingWindowInDays())
                .table(table)
                .build();
    }
}
