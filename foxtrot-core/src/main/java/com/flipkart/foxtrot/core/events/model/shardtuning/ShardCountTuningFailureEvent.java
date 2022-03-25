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
public class ShardCountTuningFailureEvent implements TrackingEvent<ShardCountTuningFailureEvent> {

    private String table;

    private int rollingWindowInDays;

    @Override
    public Event<ShardCountTuningFailureEvent> toIngestionEvent() {
        return Event.<ShardCountTuningFailureEvent>builder().groupingKey(table)
                .eventType("SHARD_COUNT_TUNING_FAILURE")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
