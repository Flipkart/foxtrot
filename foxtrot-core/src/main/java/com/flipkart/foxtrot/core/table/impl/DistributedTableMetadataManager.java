/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.table.impl;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.cache.impl.DistributedCache;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:11 PM
 */

public class DistributedTableMetadataManager implements TableMetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(DistributedTableMetadataManager.class);
    public static final String DATA_MAP = "tablemetadatamap";
    private final HazelcastConnection hazelcastConnection;
    private IMap<String, Table> tableDataStore;

    public DistributedTableMetadataManager(HazelcastConnection hazelcastConnection,
                                           ElasticsearchConnection elasticsearchConnection) {
        this.hazelcastConnection = hazelcastConnection;
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(TableMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        MapConfig mapConfig = new MapConfig(DATA_MAP);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        mapConfig.setReadBackupData(true);
        hazelcastConnection.getHazelcastConfig().addMapConfig(mapConfig);
        DistributedCache.setupConfig(hazelcastConnection);
    }


    @Override
    public void save(Table table) throws FoxtrotException {
        logger.info(String.format("Saving Table : %s", table));
        tableDataStore.put(table.getName(), table);
        tableDataStore.flush();
    }

    @Override
    public Table get(String tableName) throws FoxtrotException {
        logger.debug(String.format("Getting Table : %s", tableName));
        if (tableDataStore.containsKey(tableName)) {
            return tableDataStore.get(tableName);
        }
        return null;
    }

    @Override
    public List<Table> get() throws FoxtrotException {
        if (0 == tableDataStore.size()) { //HACK::Check https://github.com/hazelcast/hazelcast/issues/1404
            return Collections.emptyList();
        }
        ArrayList<Table> tables = Lists.newArrayList(tableDataStore.values());
        Collections.sort(tables, (lhs, rhs) -> lhs.getName().toLowerCase().compareTo(rhs.getName().toLowerCase()));
        return tables;
    }

    @Override
    public boolean exists(String tableName) throws FoxtrotException {
        return tableDataStore.containsKey(tableName);
    }

    @Override
    public void delete(String tableName) throws FoxtrotException {
        logger.info(String.format("Deleting Table : %s", tableName));
        if (tableDataStore.containsKey(tableName)) {
            tableDataStore.delete(tableName);
        }
        logger.info(String.format("Deleted Table : %s", tableName));
    }

    @Override
    public void start() throws Exception {
        tableDataStore = hazelcastConnection.getHazelcast().getMap(DATA_MAP);
    }

    @Override
    public void stop() throws Exception {
    }
}
