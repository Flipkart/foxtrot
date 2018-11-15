package com.flipkart.foxtrot.core.querystore.actions;
/*
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

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.glassfish.hk2.api.MultiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/***
 Created by nitish.goyal on 22/08/18
 ***/
@AnalyticsProvider(opcode = "multiQuery", request = MultiQueryRequest.class, response = MultiQueryResponse.class, cacheable = false)
public class MultiQueryAction extends Action<MultiQueryRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiQueryAction.class);
    private AnalyticsLoader analyticsLoader;
    private Map<ActionRequest, Action> requestActionMap = Maps.newHashMap();

    public MultiQueryAction(MultiQueryRequest parameter, TableMetadataManager tableMetadataManager,
                            DataStore dataStore, QueryStore queryStore,
                            ElasticsearchConnection connection,
                            String cacheToken, CacheManager cacheManager,
                            ObjectMapper objectMapper, AnalyticsLoader analyticsLoader) {
        super(parameter, tableMetadataManager, dataStore, queryStore,
              connection, cacheToken, cacheManager, objectMapper);
        this.analyticsLoader = analyticsLoader;
    }

    @Override
    public void preprocess() {
        MultiQueryRequest multiQueryRequest = getParameter();
        processForSubQueries(multiQueryRequest, (action, request) -> {
            action.preprocess();
            return null;
        });
    }

    @Override
    public String getMetricKey() {
        return processForSubQueries(getParameter(), (action, request) -> action.getMetricKey());
    }

    @Override
    public String getRequestCacheKey() {
        return processForSubQueries(getParameter(), (action, request) -> action.getRequestCacheKey());
    }

    @Override
    public void validateImpl(MultiQueryRequest parameter) throws MalformedQueryException {
        MultiQueryRequest multiQueryRequest = getParameter();
        MultiException multiException = new MultiException();
        processForSubQueries(multiQueryRequest, (action, request) -> {
            try {
                action.validateImpl(request);
            } catch (MalformedQueryException e) {
                multiException.addError(e);
            }
            return null;
        });
        if (CollectionUtils.isNotEmpty(multiException.getErrors())) {
            throw multiException;
        }

    }

    @Override
    public ActionResponse execute(MultiQueryRequest parameter) throws FoxtrotException {


        MultiSearchRequestBuilder multiSearchRequestBuilder = getRequestBuilder(parameter);

        Map<String, ActionResponse> queryVsQueryResponseMap = Maps.newHashMap();
        try {
            LOGGER.info("Search: {}", multiSearchRequestBuilder);
            MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.execute().actionGet();
            return getResponse(multiSearchResponse, parameter);
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }

    }

    @Override
    public MultiSearchRequestBuilder getRequestBuilder(MultiQueryRequest parameter) throws FoxtrotException {

        MultiSearchRequestBuilder multiSearchRequestBuilder = getConnection().getClient().prepareMultiSearch();

        for (Map.Entry<String, ActionRequest> entry : parameter.getRequests().entrySet()) {
            try {
                ActionRequest request = entry.getValue();
                Action<ActionRequest> action = analyticsLoader.getAction(request);
                if (null == action) {
                    throw FoxtrotExceptions.queryCreationException(request, null);
                }
                ActionRequestBuilder requestBuilder = action.getRequestBuilder(request);
                if (requestBuilder instanceof SearchRequestBuilder) {
                    multiSearchRequestBuilder.add((SearchRequestBuilder) requestBuilder);
                }
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(parameter, e);
            }
        }
        return multiSearchRequestBuilder;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse multiSearchResponse, MultiQueryRequest parameter)
            throws FoxtrotException {

        MultiQueryResponse multiQueryResponse = new MultiQueryResponse(Opcodes.MULTI_QUERY);
        Map<String, ActionResponse> queryVsQueryResponseMap = Maps.newHashMap();
        int queryCounter = 0;
        List<String> queryKeys = Lists.newArrayList();
        List<ActionRequest> requests = Lists.newArrayList();
        for (Map.Entry<String, ActionRequest> entry : getParameter().getRequests().entrySet()) {
            queryKeys.add(entry.getKey());
            requests.add(entry.getValue());
        }
        for (MultiSearchResponse.Item item : ((MultiSearchResponse) multiSearchResponse).getResponses()) {
            Action action = null;
            ActionRequest request = requests.get(queryCounter);
            try {
                action = analyticsLoader.getAction(request);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (null == action) {
                throw FoxtrotExceptions.queryCreationException(request, null);
            }
            String key = queryKeys.get(queryCounter++);
            ActionResponse response = action.getResponse(item.getResponse(), request);
            queryVsQueryResponseMap.put(key, response);
        }
        multiQueryResponse.setResponses(queryVsQueryResponseMap);
        return multiQueryResponse;
    }

    private String processForSubQueries(MultiQueryRequest multiQueryRequest, ActionInterface actionInterface) {
        List<String> results = Lists.newArrayList();
        for (Map.Entry<String, ActionRequest> entry : multiQueryRequest.getRequests().entrySet()) {
            ActionRequest request = entry.getValue();
            Action action;
            try {
                if (requestActionMap.get(request) != null) {
                    action = requestActionMap.get(request);
                } else {
                    action = analyticsLoader.getAction(request);
                    requestActionMap.put(request, action);
                }
            } catch (Exception e) {
                throw new RuntimeException("No action found for the sub request : " + request.toString());
            }
            if (null == action) {
                throw new RuntimeException("No action found for the sub request : " + request.toString());
            }
            String result = actionInterface.invoke(action, request);
            if (StringUtils.isNotBlank(result)) {
                results.add(result);
            }
        }
        return Joiner.on("-").join(results);
    }
}