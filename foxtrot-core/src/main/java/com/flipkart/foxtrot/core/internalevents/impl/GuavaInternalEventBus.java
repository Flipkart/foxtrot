package com.flipkart.foxtrot.core.internalevents.impl;

import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;

import java.util.concurrent.Executors;

/**
 *
 */
public class GuavaInternalEventBus implements InternalEventBus {

    private final AsyncEventBus eventBus;

    public GuavaInternalEventBus() {
        eventBus = new AsyncEventBus(Executors.newFixedThreadPool(10));
    }

    @Override
    public void publish(InternalSystemEvent systemEvent) {
        eventBus.post(systemEvent);
    }

    @Override
    public void subscribe(InternalEventBusConsumer consumer) {
        eventBus.register(new Subscriber(consumer));
    }

    private class Subscriber {
        private final InternalEventBusConsumer consumer;

        private Subscriber(InternalEventBusConsumer consumer) {
            this.consumer = consumer;
        }

        @Subscribe
        public void receive(InternalSystemEvent event) {
            consumer.process(event);
        }
    }
}
