package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.common.enums.SourceType;

public interface QueryManager {

    void blacklistConsole(String consoleId);

    void whitelistConsole(String consoleId);

    void blacklistUserId(String userId);

    void whitelistUserId(String userId);

    void blacklistSourceType(String sourceType);

    void whitelistSourceType(String sourceType);

    void checkIfQueryAllowed(String fqlQuery,
                             SourceType sourceType);
}
