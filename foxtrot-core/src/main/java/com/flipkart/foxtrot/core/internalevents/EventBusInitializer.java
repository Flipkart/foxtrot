package com.flipkart.foxtrot.core.internalevents;

import io.dropwizard.lifecycle.Managed;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@Order(40)
public class EventBusInitializer implements Managed {

    private final InternalEventBus eventBus;
    private final InternalEventBusConsumer eventBusConsumer;

    @Inject
    public EventBusInitializer(InternalEventBus eventBus,
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
        // do nothing
    }
}
