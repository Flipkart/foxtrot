package com.flipkart.foxtrot.core.internalevents.events;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEventVisitor;
import lombok.Data;

/**
 *
 */
@Data
public class QueryProcessed implements InternalSystemEvent {
    private final ActionRequest request;
    private final ActionResponse response;
    private final long evaluationDurationMs;

    @Override
    public <T> T accept(InternalSystemEventVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
