package com.flipkart.foxtrot.core.cache;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.Cache;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class TestCache implements Cache{
    private Map<String, ActionResponse> data = Maps.newHashMap();

    @Override
    public ActionResponse put(String key, ActionResponse data) {
        this.data.put(key, data);
        return data;
    }

    @Override
    public ActionResponse get(String key) {
        if(data.containsKey(key)) {
            return data.get(key);
        }
        return null;
    }

    @Override
    public boolean has(String key) {
        return data.containsKey(key);
    }
}
