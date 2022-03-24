package com.flipkart.foxtrot.core.tenant.impl;

import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.tenant.TenantMetadataManager;
import com.google.common.collect.Lists;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Singleton
@Slf4j
public class DistributedTenantMetadataManager implements TenantMetadataManager {

    private static final String TENANT_DATA_MAP = "tenantmetadatamap";

    private static final int TIME_TO_LIVE_TENANT_CACHE = (int) TimeUnit.DAYS.toSeconds(30);


    private final HazelcastConnection hazelcastConnection;
    private final ElasticsearchConnection elasticsearchConnection;
    private IMap<String, Tenant> tenantDataStore;

    @Inject
    public DistributedTenantMetadataManager(HazelcastConnection hazelcastConnection,
                                            ElasticsearchConnection elasticsearchConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;

        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(tenantMapConfig());
    }


    private MapConfig tenantMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(TENANT_DATA_MAP);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setReadBackupData(true);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_TENANT_CACHE);
        mapConfig.setBackupCount(0);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(TenantMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_TENANT_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        return mapConfig;
    }

    @Override
    @MonitoredFunction
    public void save(Tenant tenant) {
        tenantDataStore.put(tenant.getTenantName(), tenant);
        tenantDataStore.flush();
    }

    @Override
    @MonitoredFunction
    public Tenant get(String tenantName) {
        if (tenantDataStore.containsKey(tenantName)) {
            return tenantDataStore.get(tenantName);
        }
        return null;
    }

    @Override
    @MonitoredFunction
    public List<String> getEmailIds(String tenantName) {
        if (tenantDataStore.containsKey(tenantName)) {
            return Lists.newArrayList(tenantDataStore.get(tenantName)
                    .getEmailIds());
        }
        return Collections.emptyList();
    }

    @Override
    @SneakyThrows
    public List<Tenant> get() {
        if (0 == tenantDataStore.size()) {
            return Collections.emptyList();
        }
        ArrayList<Tenant> tenants = Lists.newArrayList(tenantDataStore.values());
        tenants.sort(Comparator.comparing(tenant -> tenant.getTenantName()
                .toLowerCase()));
        return tenants;
    }


    @Override
    public boolean exists(String tenantName) {
        return tenantDataStore.containsKey(tenantName);
    }

    @Override
    public void start() {
        tenantDataStore = hazelcastConnection.getHazelcast()
                .getMap(TENANT_DATA_MAP);
    }

    @Override
    public void stop() {
        //do nothing
    }

}
