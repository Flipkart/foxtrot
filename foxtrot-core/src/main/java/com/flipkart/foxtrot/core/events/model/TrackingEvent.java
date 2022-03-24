package com.flipkart.foxtrot.core.events.model;

public interface TrackingEvent<T> {

    Event<T> toIngestionEvent();
}

