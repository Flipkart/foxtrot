package com.flipkart.foxtrot.core.events.model.shardtuning;

import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import com.flipkart.foxtrot.core.events.model.Event;
import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.UUID;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
public class ShardCountTuningAuditEvent extends ShardCountTuningEvent implements
        TrackingEvent<ShardCountTuningAuditEvent> {

    @Builder
    public ShardCountTuningAuditEvent(String table,
                                      ShardCountTuningJobConfig shardCountTuningJobConfig,
                                      double maxIndexSizeInGBs,
                                      int idealShardCount,
                                      NodeGroupMetadata nodeGroupMetadata,
                                      String dateVsIndexSize) {
        super("SHARD_COUNT_TUNING_AUDIT", table, shardCountTuningJobConfig, maxIndexSizeInGBs, idealShardCount,
                nodeGroupMetadata, dateVsIndexSize);
    }

    @Override
    public Event<ShardCountTuningAuditEvent> toIngestionEvent() {
        return Event.<ShardCountTuningAuditEvent>builder().groupingKey(this.getTable())
                .eventType("SHARD_COUNT_TUNING_AUDIT")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
