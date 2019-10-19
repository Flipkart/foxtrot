package com.flipkart.foxtrot.core.internalevents;

/**
 *
 */
public interface InternalEventBus {
    void publish(final InternalSystemEvent systemEvent);
    void subscribe(final InternalEventBusConsumer consumer);
}
