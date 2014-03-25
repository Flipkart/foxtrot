package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.query.CachableResponseGenerator;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<ParameterType extends CachableResponseGenerator, ReturnType extends Serializable> implements Callable<String> {
    private ParameterType parameter;
    private Cache<ReturnType> cache;

    protected Action(ParameterType parameter) {
        this.parameter = parameter;
        this.cache = (isCachable())? CacheUtils.<ReturnType>getCacheFor(getName()) : null;
    }

    protected Action() {
        this.cache = (isCachable())? CacheUtils.<ReturnType>getCacheFor(getName()) : null;
    }

    public String cacheKey() {
        return parameter.getCachekey();
    }

    public String execute(ExecutorService executor) {
        executor.submit(this);
        return cacheKey();
    }

    public String execute(ParameterType parameter, ExecutorService executor) {
        this.parameter = parameter;
        executor.submit(this);
        return cacheKey();
    }

    public ReturnType get(String key) throws QueryStoreException {
        if(cache.has(key)) {
            return cache.get(key);
        }
        return null;
    }

    @Override
    public String call() throws Exception {
        final String cacheKey = cacheKey();
        cache.put(cacheKey, execute(parameter));
        return cacheKey;
    }

    public ReturnType execute() throws QueryStoreException {
        if(isCachable()) {
            if(cache.has(parameter.getCachekey())) {
                return cache.get(parameter.getCachekey());
            }
        }
        ReturnType result = execute(parameter);
        if(isCachable()) {
            return cache.put(parameter.getCachekey(), result);
        }
        return result;
    }

    abstract public boolean isCachable();
    abstract public ReturnType execute(ParameterType parameter) throws QueryStoreException;

    abstract public String getName();
}
