package com.flipkart.foxtrot.server.console;

public class ConsolePersistenceException extends Exception {
    public ConsolePersistenceException(String message) {
        super(message);
    }

    public ConsolePersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
