package com.flipkart.foxtrot.core.events;

import com.flipkart.foxtrot.core.events.model.Event;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Singleton;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class EventIngestionClient implements Managed {

    @Subscribe
    public void publishEvent(Event event) {
        try {
            //Do Nothing
        } catch (Exception e) {
            log.error(String.format("Error ingesting event for reference %s", event.getGroupingKey()), e);
        }
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
