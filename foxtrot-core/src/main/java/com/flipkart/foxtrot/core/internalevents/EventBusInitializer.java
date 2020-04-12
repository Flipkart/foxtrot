package com.flipkart.foxtrot.core.internalevents;

import io.dropwizard.lifecycle.Managed;

/**
 *
 */
public class EventBusInitializer implements Managed {

    private final InternalEventBus eventBus;
    private final InternalEventBusConsumer eventBusConsumer;

    public EventBusInitializer(
            InternalEventBus eventBus,
            InternalEventBusConsumer eventBusConsumer) {
        this.eventBus = eventBus;
        this.eventBusConsumer = eventBusConsumer;
    }

    @Override
    public void start() throws Exception {
        eventBus.subscribe(eventBusConsumer);
    }

    @Override
    public void stop() throws Exception {

    }
}
