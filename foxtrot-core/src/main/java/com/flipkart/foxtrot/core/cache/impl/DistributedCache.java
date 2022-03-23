/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.cache.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.util.Constants;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.flipkart.foxtrot.core.querystore.actions.Constants.CACHE_NAME_PREFIX;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:43 PM
 */

public class DistributedCache implements Cache {

    private static final Logger logger = LoggerFactory.getLogger(DistributedCache.class.getSimpleName());
    private final IMap<String, String> distributedMap;
    private final ObjectMapper mapper;

    public DistributedCache(HazelcastConnection hazelcastConnection, String name, ObjectMapper mapper) {
        this.distributedMap = hazelcastConnection.getHazelcast()
                .getMap(CACHE_NAME_PREFIX + name);
        this.mapper = mapper;
    }

    @Override
    public ActionResponse put(String key, ActionResponse data) {
        try {
            final String serializedData = mapper.writeValueAsString(data);
            if (serializedData != null) {
                // Only cache if size is less that 256 KB
                if (serializedData.length() <= Constants.CACHE_VALUE_SIZE_IN_KB) {
                    distributedMap.put(key, mapper.writeValueAsString(data));
                } else {
                    String responsePart = serializedData.substring(0, 1024);
                    logger.error("Size of response is too big for cache. Skipping it. Response Part : {}",
                            responsePart
                    );
                }
            }
        } catch (JsonProcessingException e) {
            logger.error("Error saving value to map: ", e);
        }
        return data;
    }

    @Override
    public ActionResponse get(String key) {
        if (null == key) {
            return null; //Hazelcast map throws NPE if key is null
        }
        String data = distributedMap.get(key);
        if (null != data) {
            try {
                return mapper.readValue(data, ActionResponse.class);
            } catch (IOException e) {
                logger.error("Error deserializing: ", e);
            }
        }
        return null;
    }

    @Override
    public boolean has(String key) {
        return null != key && distributedMap.containsKey(key);
    }
}
