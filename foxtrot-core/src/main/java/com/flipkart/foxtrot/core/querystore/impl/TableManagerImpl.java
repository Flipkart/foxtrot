package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.TableManager;
import com.hazelcast.core.IMap;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:11 PM
 */
public class TableManagerImpl implements TableManager {
    private HazelcastConnection hazelcastConnection;
    private IMap<String, Table> tableDataStore;

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
}
