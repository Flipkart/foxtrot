package com.flipkart.foxtrot.core.cache;

import com.flipkart.foxtrot.core.common.Cache;
import com.flipkart.foxtrot.core.common.CacheFactory;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class TestCacheFactory implements CacheFactory{
    @Override
    public Cache create(String name) {
        return new TestCache();
    }
}
