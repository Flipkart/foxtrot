package com.flipkart.foxtrot.core.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 26/12/15.
 */

public class CacheManager {

    private final Map<String, Cache> cacheMap = new HashMap<>();
    private CacheFactory cacheFactory;

    public CacheManager(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    public void create(String name) {
        cacheMap.put(name, cacheFactory.create(name));
    }

    public Cache getCacheFor(String name) {
        return cacheMap.get(name);
    }
}
