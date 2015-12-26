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
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:51 PM
 */

public class DistributedCacheFactory implements CacheFactory {
    private final HazelcastConnection connection;
    private final ObjectMapper mapper;

    public DistributedCacheFactory(HazelcastConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public Cache create(String name) {
        return new DistributedCache(connection, name, mapper);
    }
}
