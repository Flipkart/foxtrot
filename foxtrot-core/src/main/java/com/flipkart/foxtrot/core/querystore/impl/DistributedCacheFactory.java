package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheFactory;

import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:51 PM
 */
public class DistributedCacheFactory<T extends ActionResponse> implements CacheFactory<T> {
    private HazelcastConnection connection;
    private ObjectMapper mapper;

    public DistributedCacheFactory(HazelcastConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public Cache<T> create(String name) {
        return new DistributedCache<T>(connection, name, mapper);
    }
}
