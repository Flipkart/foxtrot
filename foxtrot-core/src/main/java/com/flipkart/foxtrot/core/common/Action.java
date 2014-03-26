package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<ParameterType extends ActionRequest> implements Callable<String> {
    private ParameterType parameter;
    private DataStore dataStore;
    private ElasticsearchConnection connection;
    private String cacheToken;
    private Cache cache;
    private String cacheKey = null;

    protected Action(ParameterType parameter, DataStore dataStore, ElasticsearchConnection connection, String cacheToken) {
        this.parameter = parameter;
        this.cacheToken = cacheToken;
        this.cache = CacheUtils.getCacheFor(this.cacheToken);
        this.connection = connection;
        this.dataStore = dataStore;
    }

    public String cacheKey() {
        if(null == cacheKey) {
            final String childKey = String.format("%s-%d", getRequestCacheKey(), System.currentTimeMillis() / 30000);
            cacheKey = UUID.nameUUIDFromBytes(childKey.getBytes()).toString();
        }
        return cacheKey;
    }

    public AsyncDataToken execute(ExecutorService executor) {
        executor.submit(this);
        return new AsyncDataToken(cacheToken,cacheKey());
    }

    public AsyncDataToken execute(ParameterType parameter, ExecutorService executor) {
        this.parameter = parameter;
        executor.submit(this);
        return new AsyncDataToken(cacheToken,cacheKey());
    }

    @Override
    public String call() throws Exception {
        final String cacheKey = cacheKey();
        cache.put(cacheKey, execute(parameter));
        return cacheKey;
    }

    public ActionResponse execute() throws QueryStoreException {
        if(isCachable()) {
            if(cache.has(cacheKey())) {
                return cache.get(cacheKey());
            }
        }
        ActionResponse result = execute(parameter);
        if(isCachable()) {
            return cache.put(cacheKey(), result);
        }
        return result;
    }

    protected ParameterType getParameter() {
        return parameter;
    }

    public final boolean isCachable() {
        return cache != null;
    }
    abstract protected String getRequestCacheKey();
    abstract public ActionResponse execute(ParameterType parameter) throws QueryStoreException;

    public DataStore getDataStore() {
        return dataStore;
    }

    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public ElasticsearchConnection getConnection() {
        return connection;
    }

    public void setConnection(ElasticsearchConnection connection) {
        this.connection = connection;
    }
}
