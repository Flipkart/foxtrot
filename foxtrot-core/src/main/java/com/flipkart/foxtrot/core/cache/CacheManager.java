package com.flipkart.foxtrot.core.cache;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Created by rishabh.goyal on 26/12/15.
 */
@Singleton
public class CacheManager {

    private final Map<String, Cache> cacheMap = new HashMap<>();
    private CacheFactory cacheFactory;

    @Inject
    public CacheManager(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    public void create(String name) {
        if (!cacheMap.containsKey(name)) {
            cacheMap.put(name, cacheFactory.create(name));
        }
    }

    public Cache getCacheFor(String name) {
        return cacheMap.get(name);
    }
}
