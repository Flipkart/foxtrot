package com.flipkart.foxtrot.server.console;

import java.util.List;

public interface ConsolePersistence {
    public void save(Console console) throws ConsolePersistenceException;
    public Console get(final String id) throws ConsolePersistenceException;
    public List<Console> get() throws ConsolePersistenceException;
}
