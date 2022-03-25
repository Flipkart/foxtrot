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
public class ShardRebalanceJobFailureEvent implements TrackingEvent<ShardRebalanceJobFailureEvent> {

    private String datePostFix;

    private String exceptionMessage;

    private String exceptionCause;

    private String exception;

    @Override
    public Event<ShardRebalanceJobFailureEvent> toIngestionEvent() {
        return Event.<ShardRebalanceJobFailureEvent>builder().groupingKey(datePostFix)
                .eventType("SHARD_REBALANCE_JOB_FAILURE")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
