package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IMap;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:11 PM
 */
public class DistributedTableMetadataManager implements TableMetadataManager {
    private static final String DATA_MAP = "tablemetadatamap";
    private HazelcastConnection hazelcastConnection;
    private ElasticsearchConnection elasticsearchConnection;
    private IMap<String, Table> tableDataStore;

    public DistributedTableMetadataManager(HazelcastConnection hazelcastConnection,
                                           ElasticsearchConnection elasticsearchConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;
    }


    @Override
    public void save(Table table) throws Exception {
        tableDataStore.put(table.getName(), table);
    }

    @Override
    public Table get(String tableName) throws Exception {
        if(tableDataStore.containsKey(tableName)) {
            return tableDataStore.get(tableName);
        }
        return null;
    }

    @Override
    public boolean exists(String tableName) throws Exception {
        return tableDataStore.containsKey(tableName);
    }

    @Override
    public void start() throws Exception {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(TableMapStore.class.getCanonicalName());
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setFactoryImplementation(TableMapStore.factory(elasticsearchConnection));
        MapConfig mapConfig = new MapConfig(DATA_MAP);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        hazelcastConnection.getHazelcast().getConfig().addMapConfig(mapConfig);
        tableDataStore = hazelcastConnection.getHazelcast().getMap(DATA_MAP);
    }

    @Override
    public void stop() throws Exception {
    }
}
