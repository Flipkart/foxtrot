/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<ParameterType extends ActionRequest> implements Callable<String> {
    private static final Logger logger = LoggerFactory.getLogger(Action.class.getSimpleName());

    private ParameterType parameter;
    private DataStore dataStore;
    private ElasticsearchConnection connection;
    private final TableMetadataManager tableMetadataManager;
    private final QueryStore queryStore;
    private final String cacheToken;
    private final CacheManager cacheManager;
    private String cacheKey = null;

    protected Action(ParameterType parameter,
                     TableMetadataManager tableMetadataManager,
                     DataStore dataStore,
                     QueryStore queryStore,
                     ElasticsearchConnection connection,
                     String cacheToken,
                     CacheManager cacheManager) {
        this.parameter = parameter;
        this.tableMetadataManager = tableMetadataManager;
        this.queryStore = queryStore;
        this.cacheToken = cacheToken;
        this.cacheManager = cacheManager;
        this.connection = connection;
        this.dataStore = dataStore;
    }

    public String cacheKey() {
        if (null == cacheKey) {
            cacheKey = String.format("%s-%d", getRequestCacheKey(), System.currentTimeMillis() / 30000);//UUID.nameUUIDFromBytes(childKey.getBytes()).toString();
        }
        return cacheKey;
    }

    public AsyncDataToken execute(ExecutorService executor) {
        executor.submit(this);
        return new AsyncDataToken(cacheToken, cacheKey());
    }

    @Override
    public String call() throws Exception {
        final String cacheKey = cacheKey();
        cacheManager.getCacheFor(this.cacheToken).put(cacheKey, execute(parameter));
        return cacheKey;
    }

    public ActionResponse execute() throws FoxtrotException {
        Cache cache = cacheManager.getCacheFor(this.cacheToken);
        final String cacheKeyValue = cacheKey();
        if (isCacheable()) {
            if (cache.has(cacheKeyValue)) {
                logger.info("Cache hit for key: " + cacheKeyValue);
                return cache.get(cacheKey());
            }
        }
        logger.info("Cache miss for key: " + cacheKeyValue);
        parameter.setFilters(checkAndAddTemporalBoundary(parameter.getFilters()));
        ActionResponse result = execute(parameter);
        if (isCacheable()) {
            logger.info("Cache load for key: " + cacheKeyValue);
            return cache.put(cacheKey(), result);
        }
        return result;
    }

    protected ParameterType getParameter() {
        return parameter;
    }

    public final boolean isCacheable() {
        return cacheManager.getCacheFor(this.cacheToken) != null;
    }

    abstract protected String getRequestCacheKey();

    abstract public ActionResponse execute(ParameterType parameter) throws FoxtrotException;

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

    public TableMetadataManager getTableMetadataManager() {
        return tableMetadataManager;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    protected Filter getDefaultTimeSpan() {
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        return lessThanFilter;
    }

    private List<Filter> checkAndAddTemporalBoundary(List<Filter> filters) {
        if (null != filters) {
            for (Filter filter : filters) {
                if (filter.isTemporal()) {
                    return filters;
                }
            }
        }
        if (null == filters) {
            filters = Lists.newArrayList();
        } else {
            filters = Lists.newArrayList(filters);
        }
        filters.add(getDefaultTimeSpan());
        return filters;
    }

}
