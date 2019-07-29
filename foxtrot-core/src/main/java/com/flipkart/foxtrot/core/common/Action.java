/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.alerts.EmailConfig;
import com.flipkart.foxtrot.core.cache.Cache;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<P extends ActionRequest> implements Callable<String> {

    private static final Logger logger = LoggerFactory.getLogger(Action.class.getSimpleName());
    private final TableMetadataManager tableMetadataManager;
    private final QueryStore queryStore;
    private final String cacheToken;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private P parameter;
    private ElasticsearchConnection connection;

    protected Action(P parameter, String cacheToken, AnalyticsLoader analyticsLoader) {
        this.parameter = parameter;
        this.tableMetadataManager = analyticsLoader.getTableMetadataManager();
        this.queryStore = analyticsLoader.getQueryStore();
        this.cacheToken = cacheToken;
        this.cacheManager = analyticsLoader.getCacheManager();
        this.connection = analyticsLoader.getElasticsearchConnection();
        this.objectMapper = analyticsLoader.getObjectMapper();
    }

    public AsyncDataToken execute(ExecutorService executor, String email) {
        preProcessRequest(email);
        executor.submit(this);
        return new AsyncDataToken(cacheToken, cacheKey());
    }

    private void preProcessRequest(String email) {
        if (parameter.getFilters() == null) {
            parameter.setFilters(Lists.newArrayList(new AnyFilter()));
        }
        preprocess();
        parameter.setFilters(checkAndAddTemporalBoundary(parameter.getFilters()));
        validateBase(parameter);
        validateImpl(parameter, email);
    }

    public String cacheKey() {
        return String.format("%s-%d", getRequestCacheKey(), System.currentTimeMillis() / 30000);
    }

    public abstract void preprocess();

    private List<Filter> checkAndAddTemporalBoundary(List<Filter> filters) {
        if (null != filters) {
            for (Filter filter : filters) {
                if (filter.isFilterTemporal()) {
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

    private void validateBase(P parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (!CollectionUtils.isNullOrEmpty(parameter.getFilters())) {
            for (Filter filter : parameter.getFilters()) {
                Set<String> errors = filter.validate();
                if (!CollectionUtils.isNullOrEmpty(errors)) {
                    validationErrors.addAll(errors);
                }
            }
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    public abstract void validateImpl(P parameter, String email);

    public abstract String getRequestCacheKey();

    protected Filter getDefaultTimeSpan() {
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        return lessThanFilter;
    }

    @Override
    public String call() throws Exception {
        final String cacheKey = cacheKey();
        cacheManager.getCacheFor(this.cacheToken)
                .put(cacheKey, execute(parameter));
        return cacheKey;
    }

    public abstract ActionResponse execute(P parameter);

    public ActionValidationResponse validate(String email) {
        try {
            preProcessRequest(email);
        } catch (MalformedQueryException e) {
            return ActionValidationResponse.builder()
                    .processedRequest(parameter)
                    .validationErrors(e.getReasons())
                    .build();
        } catch (Exception e) {
            return ActionValidationResponse.builder()
                    .processedRequest(parameter)
                    .validationErrors(Collections.singletonList(e.getMessage()))
                    .build();
        }
        return ActionValidationResponse.builder()
                .processedRequest(parameter)
                .validationErrors(Collections.emptyList())
                .build();
    }

    public ActionResponse execute(String email) {
        preProcessRequest(email);
        ActionResponse cachedData = readCachedData();
        if(cachedData != null) {
            return cachedData;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            ActionResponse result = execute(parameter);
            // Publish success metrics
            final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            MetricUtil.getInstance()
                    .registerActionSuccess(cacheToken, getMetricKey(), elapsed);
            if (elapsed > 1000) {
                logSlowQuery(elapsed);
            }

            // Now cache data
            updateCachedData(result);

            return result;
        } catch (FoxtrotException e) {
            stopwatch.stop();
            // Publish failure metrics
            MetricUtil.getInstance()
                    .registerActionFailure(cacheToken, getMetricKey(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private void logSlowQuery(final long elapsed) {
        try {
            String query = getObjectMapper().writeValueAsString(parameter);
            logger.warn("SLOW_QUERY: Time: {} ms Query: {}", elapsed, query);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing slow query", e);
        }
    }

    public long getGetQueryTimeout() {
        if (getConnection().getConfig() == null) {
            return ElasticsearchConfig.DEFAULT_TIMEOUT;
        }
        return getConnection().getConfig()
                .getGetQueryTimeout();
    }

    public ElasticsearchConnection getConnection() {
        return connection;
    }
    
    private void updateCachedData(ActionResponse result) {
        Cache cache = cacheManager.getCacheFor(this.cacheToken);
        if(isCacheable()) {
            cache.put(cacheKey(), result);
        }
    }

    protected ActionResponse readCachedData() {
        Cache cache = cacheManager.getCacheFor(this.cacheToken);
        final String cacheKeyValue = cacheKey();
        if (isCacheable()) {
            if (cache.has(cacheKeyValue)) {
                MetricUtil.getInstance()
                        .registerActionCacheHit(cacheToken, getMetricKey());
                logger.info("Cache hit for key: {}", cacheKeyValue);
                return cache.get(cacheKey());
            } else {
                MetricUtil.getInstance()
                        .registerActionCacheMiss(cacheToken, getMetricKey());
                logger.info("Cache miss for key: {}", cacheKeyValue);
            }
        }
        return null;
    }

    /**
     * Returns a metric key for current action. Ideally this key's cardinality should be less since each new value of
     * this key will create new JMX metric
     * <p>
     * Sample use cases - Used for reporting per action
     * success/failure metrics
     * cache hit/miss metrics
     *
     * @return metric key for current action
     */
    public abstract String getMetricKey();

    public abstract ActionRequestBuilder getRequestBuilder(P parameter);

    public abstract ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, P parameter);

    protected P getParameter() {
        return parameter;
    }

    public final boolean isCacheable() {
        return cacheManager.getCacheFor(this.cacheToken) != null;
    }

    public TableMetadataManager getTableMetadataManager() {
        return tableMetadataManager;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

}
