package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheFactory;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:51 PM
 */
public class DistributedCacheFactory implements CacheFactory {
    private HazelcastConnection connection;
    private ObjectMapper mapper;

    public DistributedCacheFactory(HazelcastConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public Cache create(String name) {
        return new DistributedCache(connection, name, mapper);
    }
}
