package com.flipkart.foxtrot.core.internalevents;

import com.flipkart.foxtrot.core.internalevents.events.QueryProcessed;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;

/**
 *
 */
public interface InternalSystemEventVisitor<T> {
    T visit(QueryProcessed queryProcessed);

    T visit(QueryProcessingError queryProcessingError);
}
