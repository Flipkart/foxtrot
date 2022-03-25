package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import io.appform.functionmetrics.MonitoredFunction;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
@Singleton
public class QueryManagerImpl implements QueryManager, Managed {

    private static final String BLACKLISTED_CONSOLES_MAP = "blacklistedConsoles";
    private static final String BLACKLISTED_USER_ID_MAP = "blacklistedUserIds";
    private static final String BLACKLISTED_SOURCE_TYPES_MAP = "blacklistedSourceTypes";
    private static final int TIME_TO_LIVE_CONSOLE_MAP = (int) TimeUnit.HOURS.toSeconds(12);
    private static final int TIME_TO_LIVE_USERS_MAP = (int) TimeUnit.HOURS.toSeconds(12);
    private static final int TIME_TO_NEAR_CACHE = (int) TimeUnit.MINUTES.toSeconds(15);
    private final HazelcastConnection hazelcastConnection;
    private IMap<String, Boolean> blacklistedConsoles;
    private IMap<String, Boolean> blacklistedUserIds;
    private IMap<String, Boolean> blackListedSourceTypes;

    @Inject
    public QueryManagerImpl(final HazelcastConnection hazelcastConnection) {
        this.hazelcastConnection = hazelcastConnection;
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(consoleMapConfig());
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(usersMapConfig());
        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(sourceTypeMapConfig());
    }

    @Override
    public void start() {
        blacklistedConsoles = hazelcastConnection.getHazelcast()
                .getMap(BLACKLISTED_CONSOLES_MAP);
        blacklistedUserIds = hazelcastConnection.getHazelcast()
                .getMap(BLACKLISTED_USER_ID_MAP);
        blackListedSourceTypes = hazelcastConnection.getHazelcast()
                .getMap(BLACKLISTED_SOURCE_TYPES_MAP);
    }

    @Override
    public void stop() {
        //do nothing
    }

    private MapConfig consoleMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(BLACKLISTED_CONSOLES_MAP);
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_CONSOLE_MAP);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_NEAR_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    private MapConfig usersMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(BLACKLISTED_USER_ID_MAP);
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_USERS_MAP);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_NEAR_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    private MapConfig sourceTypeMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(BLACKLISTED_SOURCE_TYPES_MAP);
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_USERS_MAP);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_NEAR_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return mapConfig;
    }

    @Override
    @MonitoredFunction
    public void blacklistConsole(String consoleId) {
        blacklistedConsoles.put(consoleId, true);
    }

    @Override
    @MonitoredFunction
    public void whitelistConsole(String consoleId) {
        blacklistedConsoles.remove(consoleId);
    }

    @Override
    public void blacklistUserId(String userId) {
        blacklistedUserIds.put(userId, true);
    }

    @Override
    public void whitelistUserId(String userId) {
        blacklistedUserIds.remove(userId);
    }

    @Override
    @MonitoredFunction
    public void blacklistSourceType(String sourceType) {
        blackListedSourceTypes.put(sourceType, true);
    }

    @Override
    @MonitoredFunction
    public void whitelistSourceType(String sourceType) {
        blackListedSourceTypes.remove(sourceType);
    }

    @Override
    public void checkIfQueryAllowed(String fqlQuery,
                                    SourceType sourceType) {
        if (isSourceTypeBlockedForRunningQuery(sourceType)) {
            log.info("Query is blocked from sourceType : {}, fql query: {}", sourceType, fqlQuery);
            throw FoxtrotExceptions.createFqlQueryBlockedException(fqlQuery);
        }
    }

    private boolean isSourceTypeBlockedForRunningQuery(SourceType sourceType) {
        return Objects.nonNull(sourceType) && isSourceTypeBlacklisted(sourceType);
    }

    private boolean isSourceTypeBlacklisted(SourceType sourceType) {
        return blackListedSourceTypes.containsKey(sourceType.name());
    }

    private boolean isUserIdBlacklisted(String userId) {
        return blacklistedUserIds.containsKey(userId);
    }

    @MonitoredFunction
    private boolean isConsoleBlacklisted(String consoleId) {
        return blacklistedConsoles.containsKey(consoleId);
    }
}
