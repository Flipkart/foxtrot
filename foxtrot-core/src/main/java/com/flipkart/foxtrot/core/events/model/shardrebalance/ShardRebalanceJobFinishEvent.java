package com.flipkart.foxtrot.core.events.model.shardrebalance;

import com.flipkart.foxtrot.core.events.model.Event;
import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.UUID;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

@Data
@Builder
public class ShardRebalanceJobFinishEvent implements TrackingEvent<ShardRebalanceJobFinishEvent> {

    private String datePostFix;

    @Override
    public Event<ShardRebalanceJobFinishEvent> toIngestionEvent() {
        return Event.<ShardRebalanceJobFinishEvent>builder().groupingKey(datePostFix)
                .eventType("SHARD_REBALANCE_JOB_FINISHED")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
