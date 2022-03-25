package com.flipkart.foxtrot.core.events.model.shardtuning;

import com.flipkart.foxtrot.core.events.model.Event;
import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.UUID;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

@Data
@Builder
public class ShardCountTuningSkipEvent implements TrackingEvent<ShardCountTuningSkipEvent> {

    private String table;

    private int rollingWindowInDays;

    private double idealShardSizeInGBs;

    private double maxIndexSizeInGBs;

    private int idealShardCount;

    private String dateVsIndexSize;

    @Override
    public Event<ShardCountTuningSkipEvent> toIngestionEvent() {
        return Event.<ShardCountTuningSkipEvent>builder().groupingKey(table)
                .eventType("SHARD_COUNT_TUNING_SKIP")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
