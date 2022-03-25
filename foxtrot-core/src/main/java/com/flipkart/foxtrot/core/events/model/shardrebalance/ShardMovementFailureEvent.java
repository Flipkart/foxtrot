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
public class ShardMovementFailureEvent implements TrackingEvent<ShardMovementFailureEvent> {

    private String nodeGroup;

    private String datePostFix;

    private String moveCommand;

    private String fromNode;

    private String toNode;

    private String exceptionMessage;

    private String exceptionCause;

    private String exception;

    @Override
    public Event<ShardMovementFailureEvent> toIngestionEvent() {
        return Event.<ShardMovementFailureEvent>builder().groupingKey(nodeGroup)
                .eventType("SHARD_MOVEMENT_FAILURE")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }
}
