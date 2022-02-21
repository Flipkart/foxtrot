package com.flipkart.foxtrot.server.auth.sessionstore;

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.google.common.base.Strings;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import io.dropwizard.lifecycle.Managed;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

/**
 *
 */
@Singleton
@Order(50)
public class DistributedSessionDataStore implements SessionDataStore, Managed {
    private static final String MAP_NAME = "REFERER_CACHE";
    private final HazelcastConnection hazelcastConnection;

    private IMap<String, Object> store;

    @Inject
    public DistributedSessionDataStore(HazelcastConnection hazelcastConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.hazelcastConnection.getHazelcastConfig()
                .getMapConfigs()
                .put(MAP_NAME, mapConfig());
    }


    @Override
    public void put(String sessionId, Object data) {
        if (Strings.isNullOrEmpty(sessionId) || null == data) {
            return;
        }
        store.put(sessionId, data);
    }

    @Override
    public Optional<Object> get(String sessionId) {
        if (Strings.isNullOrEmpty(sessionId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(store.get(sessionId));
    }

    @Override
    public void delete(String sessionId) {
        if (Strings.isNullOrEmpty(sessionId)) {
            return;
        }
        store.delete(sessionId);
    }

    @Override
    public void start() throws Exception {
        this.store = this.hazelcastConnection.getHazelcast()
                .getMap(MAP_NAME);
    }

    @Override
    public void stop() throws Exception {
        //Do nothing
    }

    private MapConfig mapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setReadBackupData(true);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(300);
        mapConfig.setBackupCount(0);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(300);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        return mapConfig;
    }
}
