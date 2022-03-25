package com.flipkart.foxtrot.core.events.model.shardtuning;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "eventType")
@JsonSubTypes({@JsonSubTypes.Type(value = ShardCountTuningAuditEvent.class, name = "SHARD_COUNT_TUNING_AUDIT"),
        @JsonSubTypes.Type(value = ShardCountTuningSuccessEvent.class, name = "SHARD_COUNT_TUNING_SUCCESS")})
@Data
@NoArgsConstructor
public abstract class ShardCountTuningEvent {

    private String eventType;

    private String table;

    private int rollingWindowInDays;

    private int replicationFactor;

    private double idealShardSizeInGBs;

    private double maxIndexSizeInGBs;

    private int idealShardCount;

    private String nodeGroup;

    private int maxShardCapacity;

    private String dateVsIndexSize;

    protected ShardCountTuningEvent(String eventType) {
        this.eventType = eventType;
    }

    public ShardCountTuningEvent(String eventType,
                                 String table,
                                 ShardCountTuningJobConfig shardCountTuningJobConfig,
                                 double maxIndexSizeInGBs,
                                 int idealShardCount,
                                 NodeGroupMetadata nodeGroupMetadata,
                                 String dateVsIndexSize) {
        this.eventType = eventType;
        this.table = table;
        this.rollingWindowInDays = shardCountTuningJobConfig.getRollingWindowInDays();
        this.replicationFactor = shardCountTuningJobConfig.getReplicationFactor();
        this.idealShardSizeInGBs = shardCountTuningJobConfig.getIdealShardSizeInGBs();
        this.maxIndexSizeInGBs = maxIndexSizeInGBs;
        this.idealShardCount = idealShardCount;
        this.nodeGroup = nodeGroupMetadata.getNodeGroup();
        this.maxShardCapacity = nodeGroupMetadata.getMaxShardCapacity();
        this.dateVsIndexSize = dateVsIndexSize;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class NodeGroupMetadata {

        private String nodeGroup;

        private int maxShardCapacity;
    }
}
