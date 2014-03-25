package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheFactory;

import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 25/03/14
 * Time: 7:51 PM
 */
public class DistributedCacheFactory<T extends Serializable> implements CacheFactory<T> {
    private HazelcastConnection connection;

    public DistributedCacheFactory(HazelcastConnection connection) {
        this.connection = connection;
    }

    @Override
    public Cache<T> create(String name) {
        return new DistributedCache<T>(connection, name);
    }
}
