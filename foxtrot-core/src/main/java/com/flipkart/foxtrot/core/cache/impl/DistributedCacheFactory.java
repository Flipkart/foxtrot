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
package com.flipkart.foxtrot.core.cache.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.hazelcast.config.*;

import static com.flipkart.foxtrot.core.querystore.actions.Constants.CACHE_NAME_PREFIX;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:51 PM
 */

public class DistributedCacheFactory implements CacheFactory {
    private final HazelcastConnection connection;
    private final ObjectMapper mapper;
    private static final int DEFAULT_TIME_TO_LIVE_SECONDS = 15;
    private static final int DEFAULT_MAX_IDLE_SECONDS = 15;
    private static final int DEFAULT_SIZE = 70;

    public DistributedCacheFactory(HazelcastConnection connection, ObjectMapper mapper, CacheConfig cacheConfig) {
        this.connection = connection;
        this.mapper = mapper;
        this.connection.getHazelcastConfig().addMapConfig(getDefaultMapConfig(cacheConfig));
    }

    @Override
    public Cache create(String name) {
        return new DistributedCache(connection, name, mapper);
    }

    private MapConfig getDefaultMapConfig(CacheConfig cacheConfig) {
        MapConfig mapConfig = new MapConfig(CACHE_NAME_PREFIX + "*");
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setBackupCount(0);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);

        if(cacheConfig.getMaxIdleSeconds() == 0) {
            mapConfig.setMaxIdleSeconds(DEFAULT_MAX_IDLE_SECONDS);
        } else {
            mapConfig.setMaxIdleSeconds(cacheConfig.getMaxIdleSeconds());
        }

        if(cacheConfig.getTimeToLiveSeconds() == 0) {
            mapConfig.setTimeToLiveSeconds(DEFAULT_TIME_TO_LIVE_SECONDS);
        } else {
            mapConfig.setTimeToLiveSeconds(cacheConfig.getTimeToLiveSeconds());
        }

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE);
        if(cacheConfig.getSize() == 0) {
            maxSizeConfig.setSize(DEFAULT_SIZE);
        } else {
            maxSizeConfig.setSize(cacheConfig.getSize());
        }
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        return mapConfig;
    }
}
