package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.core.common.Cache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IMap;

import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:43 PM
 */
public class DistributedCache<T extends Serializable> implements Cache<T> {
    IMap<String, T> distributedMap;

    public DistributedCache(HazelcastConnection hazelcastConnection, String name) {
        MapConfig mapConfig = hazelcastConnection.getHazelcast().getConfig().getMapConfig(name);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setTimeToLiveSeconds(30);
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mapConfig.setBackupCount(0);
        distributedMap = hazelcastConnection.getHazelcast().getMap(name);
    }

    @Override
    public T put(String key, T data) {
        return distributedMap.put(key, data);
    }

    @Override
    public T get(String key) {
        if(null == key) {
            return null; //Hazelcast map throws NPE if key is null
        }
        return distributedMap.get(key);
    }

    @Override
    public boolean has(String key) {
        return null != key && distributedMap.containsKey(key);
    }
}
