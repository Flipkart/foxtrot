package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.query.CachableResponseGenerator;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<ParameterType extends CachableResponseGenerator, ReturnType extends ActionResponse> implements Callable<String> {
    private static final Logger logger = LoggerFactory.getLogger(Action.class.getSimpleName());

    private ParameterType parameter;
    private Cache<ReturnType> cache;
    private String cacheKey = null;

    public static <T extends ActionResponse> T getResponse(AsyncDataToken dataToken) {
        Cache<T> cache = CacheUtils.getCacheFor(dataToken.getAction());
        if(cache.has(dataToken.getKey())) {
            return cache.get(dataToken.getKey());
        }
        return null;
    }

    protected Action(ParameterType parameter) {
        this.parameter = parameter;
        this.cache = (isCachable())? CacheUtils.<ReturnType>getCacheFor(getName()) : null;
    }

    protected Action() {
        this.cache = (isCachable())? CacheUtils.<ReturnType>getCacheFor(getName()) : null;
    }

    public String cacheKey() {
        if(null == cacheKey) {
            cacheKey = String.format("%s-%d", parameter.getCachekey(),
                                            System.currentTimeMillis()/30000);
            System.out.println("CK: " + cacheKey);
        }
        return cacheKey;
    }

    public AsyncDataToken execute(ExecutorService executor) {
        executor.submit(this);
        return new AsyncDataToken(getName(),cacheKey());
    }

    public AsyncDataToken execute(ParameterType parameter, ExecutorService executor) {
        this.parameter = parameter;
        executor.submit(this);
        return new AsyncDataToken(getName(),cacheKey());
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
            if(cache.has(cacheKey())) {
                return cache.get(cacheKey());
            }
        }
        ReturnType result = execute(parameter);
        if(isCachable()) {
            return cache.put(cacheKey(), result);
        }
        return result;
    }

    abstract public boolean isCachable();
    abstract public ReturnType execute(ParameterType parameter) throws QueryStoreException;

    abstract public String getName();
}
