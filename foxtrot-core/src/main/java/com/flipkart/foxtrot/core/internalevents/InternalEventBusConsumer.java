package com.flipkart.foxtrot.core.internalevents;

/**
 *
 */
public interface InternalEventBusConsumer {
    void process(InternalSystemEvent event);
}
