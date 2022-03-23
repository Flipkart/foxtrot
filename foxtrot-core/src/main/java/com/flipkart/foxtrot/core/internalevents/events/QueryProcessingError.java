package com.flipkart.foxtrot.core.internalevents.events;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEventVisitor;
import lombok.Data;

/**
 *
 */
@Data
public class QueryProcessingError implements InternalSystemEvent {
    private final ActionRequest request;
    private final FoxtrotException exception;

    @Override
    public <T> T accept(InternalSystemEventVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
