/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ActionValidationResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.exception.MalformedQueryException;
import com.flipkart.foxtrot.common.query.CacheKeyVisitor;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.elasticsearch.action.ActionRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:23 AM
 */
public abstract class Action<P extends ActionRequest> {

    private static final Logger logger = LoggerFactory.getLogger(Action.class.getSimpleName());
    private final TableMetadataManager tableMetadataManager;
    private final QueryStore queryStore;
    private final ObjectMapper objectMapper;
    private P parameter;
    private ElasticsearchConnection connection;
    private CacheKeyVisitor cacheKeyVisitor;

    protected Action(P parameter,
                     AnalyticsLoader analyticsLoader) {
        this.parameter = parameter;
        this.tableMetadataManager = analyticsLoader.getTableMetadataManager();
        this.queryStore = analyticsLoader.getQueryStore();
        this.connection = analyticsLoader.getElasticsearchConnection();
        this.objectMapper = analyticsLoader.getObjectMapper();
        this.cacheKeyVisitor = new CacheKeyVisitor();
    }

    public String cacheKey() {
        return String.format("%s-%d", getRequestCacheKey(), System.currentTimeMillis() / 30000);
    }

    private void preProcessRequest() {
        if (parameter.getFilters() == null) {
            parameter.setFilters(Lists.newArrayList(new AnyFilter()));
        }
        preprocess();
        parameter.setFilters(checkAndAddTemporalBoundary(parameter.getFilters()));
        validateBase(parameter);
        validateImpl(parameter);
    }

    public abstract void preprocess();

    public ActionValidationResponse validate() {
        try {
            preProcessRequest();
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

    public ActionResponse execute() {
        preProcessRequest();
        return execute(parameter);
    }

    public long getGetQueryTimeout() {
        if (getConnection().getConfig() == null) {
            return ElasticsearchConfig.DEFAULT_TIMEOUT;
        }
        return getConnection().getConfig()
                .getGetQueryTimeout();
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

    public abstract String getRequestCacheKey();

    public abstract ActionRequestBuilder getRequestBuilder(P parameter);

    public abstract ActionResponse getResponse(org.elasticsearch.action.ActionResponse response,
                                               P parameter);


    public abstract void validateImpl(P parameter);

    public abstract ActionResponse execute(P parameter);

    protected P getParameter() {
        return parameter;
    }

    public ElasticsearchConnection getConnection() {
        return connection;
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

    public CacheKeyVisitor getCacheKeyVisitor() {
        return cacheKeyVisitor;
    }

    protected Filter getDefaultTimeSpan() {
        LessThanFilter lessThanFilter = new LessThanFilter();
        lessThanFilter.setTemporal(true);
        lessThanFilter.setField("_timestamp");
        lessThanFilter.setValue(System.currentTimeMillis());
        return lessThanFilter;
    }

    protected String requestString() {
        try {
            return objectMapper.writeValueAsString(parameter);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing request: ", e);
            return "";
        }
    }

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

}
