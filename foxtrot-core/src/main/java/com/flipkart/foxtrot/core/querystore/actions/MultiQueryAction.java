package com.flipkart.foxtrot.core.querystore.actions;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.glassfish.hk2.api.MultiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/***
 Created by nitish.goyal on 22/08/18
 ***/
@AnalyticsProvider(opcode = "multi_query", request = MultiQueryRequest.class, response = MultiQueryResponse.class, cacheable = true)
public class MultiQueryAction extends Action<MultiQueryRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiQueryAction.class);
    private AnalyticsLoader analyticsLoader;
    private Map<ActionRequest, Action> requestActionMap = Maps.newHashMap();

    public MultiQueryAction(MultiQueryRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.analyticsLoader = analyticsLoader;
    }

    @Override
    public void preprocess() {
        final MultiQueryRequest multiQueryRequest = getParameter();
        createActions(multiQueryRequest);
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
    public void validateImpl(MultiQueryRequest parameter) {
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
        if(CollectionUtils.isNotEmpty(multiException.getErrors())) {
            throw multiException;
        }

    }

    @Override
    public ActionResponse execute(MultiQueryRequest parameter) {
        MultiSearchRequestBuilder multiSearchRequestBuilder = getRequestBuilder(parameter);
        try {
            LOGGER.info("Search: {}", multiSearchRequestBuilder);
            MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.execute()
                    .actionGet();
            return getResponse(multiSearchResponse, parameter);
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public MultiSearchRequestBuilder getRequestBuilder(MultiQueryRequest parameter) {

        MultiSearchRequestBuilder multiSearchRequestBuilder = getConnection().getClient()
                .prepareMultiSearch();

        for(Map.Entry<String, ActionRequest> entry : parameter.getRequests()
                .entrySet()) {
            ActionRequest request = entry.getValue();
            Action<ActionRequest> action = analyticsLoader.getAction(request);
            if(null == action) {
                throw FoxtrotExceptions.queryCreationException(request, null);
            }
            ActionRequestBuilder requestBuilder = action.getRequestBuilder(request);
            if(requestBuilder instanceof SearchRequestBuilder) {
                multiSearchRequestBuilder.add((SearchRequestBuilder)requestBuilder);
            }
        }
        return multiSearchRequestBuilder;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse multiSearchResponse, MultiQueryRequest parameter) {

        Map<String, ActionResponse> queryVsQueryResponseMap = Maps.newHashMap();
        int queryCounter = 0;
        List<String> queryKeys = Lists.newArrayList();
        List<ActionRequest> requests = Lists.newArrayList();
        for(Map.Entry<String, ActionRequest> entry : getParameter().getRequests()
                .entrySet()) {
            queryKeys.add(entry.getKey());
            requests.add(entry.getValue());
        }
        for(MultiSearchResponse.Item item : ((MultiSearchResponse)multiSearchResponse).getResponses()) {
            Action action = null;
            ActionRequest request = requests.get(queryCounter);
            try {
                action = analyticsLoader.getAction(request);
            } catch (Exception e) {
                LOGGER.error("Error occurred while executing multiQuery request : {}", e);
            }
            if(null == action) {
                throw FoxtrotExceptions.queryCreationException(request, null);
            }
            String key = queryKeys.get(queryCounter++);
            ActionResponse response = action.getResponse(item.getResponse(), request);
            queryVsQueryResponseMap.put(key, response);
        }
        return new MultiQueryResponse(queryVsQueryResponseMap);
    }

    private void createActions(final MultiQueryRequest multiQueryRequest) {
        for(Map.Entry<String, ActionRequest> entry : multiQueryRequest.getRequests().entrySet()) {
            ActionRequest request = entry.getValue();
            Action action;
            if (requestActionMap.get(request) != null) {
                action = requestActionMap.get(request);
            }
            else {
                action = analyticsLoader.getAction(request);
                requestActionMap.put(request, action);
            }
            if (null == action) {
                throw FoxtrotExceptions.createMalformedQueryException(multiQueryRequest, Collections.singletonList(
                        "No action found for the sub request : " + request.toString()));
            }
        }
    }

    private String processForSubQueries(MultiQueryRequest multiQueryRequest, ActionInterface actionInterface) {
        List<String> results = Lists.newArrayList();
        for(Map.Entry<String, ActionRequest> entry : multiQueryRequest.getRequests().entrySet()) {
            String result = actionInterface.invoke(requestActionMap.get(entry.getValue()), entry.getValue());
            if(!Strings.isNullOrEmpty(result)) {
                results.add(result);
            }
        }
        return String.join("-", results);
    }
}