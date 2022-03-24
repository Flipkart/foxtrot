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
public class ShardTuningJobFinishEvent implements TrackingEvent<ShardTuningJobFinishEvent> {

    @Override
    public Event<ShardTuningJobFinishEvent> toIngestionEvent() {
        return Event.<ShardTuningJobFinishEvent>builder().groupingKey(UUID.randomUUID()
                .toString())
                .eventType("SHARD_COUNT_TUNING_JOB_FINISHED")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
