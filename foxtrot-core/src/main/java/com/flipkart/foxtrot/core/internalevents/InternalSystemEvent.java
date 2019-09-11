package com.flipkart.foxtrot.core.internalevents;

/**
 *
 */
public interface InternalSystemEvent {

    <T> T accept(InternalSystemEventVisitor<T> visitor);
}
