package com.flipkart.foxtrot.core.common;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 5:20 PM
 */
public class CacheUtils {
    private static final class LocalCache<T> implements Cache<T> {

        @Override
        public T put(String key, T data) {
            return null;
        }

        @Override
        public T get(String key) {
            return null;
        }

        @Override
        public boolean has(String key) {
            return false;
        }
    }

    private static Map<String, Cache> cacheMap = new HashMap<String, Cache>();

    private static CacheFactory cacheFactory = null;

    public static void setCacheFactory(CacheFactory cacheFactory) {
        CacheUtils.cacheFactory = cacheFactory;
    }

    @SuppressWarnings("unchecked")
    private static<T> Cache<T> create() {
        return cacheFactory.create();
    }

    @SuppressWarnings("unchecked")
    public static <T> Cache<T> getCacheFor(String name) {
        if(!cacheMap.containsKey(name)) {
            cacheMap.put(name, create());
        }
        return cacheMap.get(name);
    }
}
