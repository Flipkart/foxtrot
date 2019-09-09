package com.flipkart.foxtrot.core.internalevents;

import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
@Slf4j
public class GuavaInternalEventBusTest {

    @Data
    private class GenericEvent implements InternalSystemEvent {
        private final String data;

        @Override
        public <T> T accept(InternalSystemEventVisitor<T> visitor) {
            return null;
        }
    }

    @Test
    public void testPubSub() {
        InternalEventBus eventBus = new GuavaInternalEventBus();
        eventBus.subscribe(event -> {
            Assert.assertEquals("Hello", ((GenericEvent)event).getData());
        });
        eventBus.publish(new GenericEvent("Hello"));
    }

}