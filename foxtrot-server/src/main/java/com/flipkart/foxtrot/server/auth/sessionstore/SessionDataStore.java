package com.flipkart.foxtrot.server.auth.sessionstore;

import java.util.Optional;

/**
 *
 */
public interface SessionDataStore {
    void put(String sessionId, Object data);
    Optional<Object> get(String sessionId);
    void delete(String sessionId);
}
