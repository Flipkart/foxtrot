package com.flipkart.foxtrot.core.common;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 5:20 PM
 */
public class CacheUtils {

    private static Map<String, Cache> cacheMap = new HashMap<String, Cache>();

    private static CacheFactory cacheFactory = null;

    public static void setCacheFactory(CacheFactory cacheFactory) {
        CacheUtils.cacheFactory = cacheFactory;
    }

    public static void create(String name) {
        cacheMap.put(name,cacheFactory.create(name));
    }

    public static Cache getCacheFor(String name) {
        return cacheMap.get(name);
    }
}
