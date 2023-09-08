package com.flipkart.foxtrot.core.querystore.actions;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.glassfish.hk2.api.MultiException;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;

/***
 Created by nitish.goyal on 22/08/18
 ***/
@AnalyticsProvider(opcode = "multi_query", request = MultiQueryRequest.class, response = MultiQueryResponse.class, cacheable = true)
@Slf4j
public class MultiQueryAction extends Action<MultiQueryRequest> {

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
        final MultiQueryRequest parameter = getParameter();
        createActions(parameter);
        long filterHashKey = 0L;
        if (null != parameter.getFilters()) {
            for (Filter filter : parameter.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        return String.format("multquery-%d-%s",
                filterHashKey,
                processForSubQueries(parameter, (action, request) -> action.getRequestCacheKey()));
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
        if (CollectionUtils.isNotEmpty(multiException.getErrors())) {
            throw multiException;
        }

    }

    @Override
    public ActionResponse execute(MultiQueryRequest parameter) {
        if (Utils.hasTemporalFilters(parameter.getFilters())) {
            val offendingRequests = parameter.getRequests().entrySet().stream()
                    .filter(entry -> Utils.hasTemporalFilters(entry.getValue().getFilters()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(offendingRequests)) {
                throw FoxtrotExceptions.createMalformedQueryException(
                        parameter,
                        Collections.singletonList(
                                "Temporal filters passed in multi query as well as children: " + offendingRequests));
            }
        }
        MultiSearchRequest multiSearchRequestBuilder = getRequestBuilder(parameter, Collections.emptyList());
        try {
            log.info("Search: {}", multiSearchRequestBuilder);
            MultiSearchResponse multiSearchResponse = getConnection()
                    .getClient()
                    .multiSearch(multiSearchRequestBuilder, RequestOptions.DEFAULT);
            return getResponse(multiSearchResponse, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public MultiSearchRequest getRequestBuilder(MultiQueryRequest parameter, List<Filter> extraFilters) {

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        val filterBuilder = ImmutableList.<Filter>builder();
        if (null != parameter.getFilters()) {
            filterBuilder.addAll(parameter.getFilters());
        }
        if (null != extraFilters) {
            filterBuilder.addAll(extraFilters);
        }
        val filters = filterBuilder.build();

        for (Map.Entry<String, ActionRequest> entry : parameter.getRequests()
                .entrySet()) {
            ActionRequest request = entry.getValue();
            Action<ActionRequest> action = analyticsLoader.getAction(request);
            if (null == action) {
                throw FoxtrotExceptions.queryCreationException(request, null);
            }
            org.opensearch.action.ActionRequest requestBuilder = action.getRequestBuilder(request, filters);
            if (requestBuilder instanceof SearchRequest) {
                multiSearchRequest.add((SearchRequest) requestBuilder);
            }
        }
        return multiSearchRequest;
    }

    @Override
    public ActionResponse getResponse(org.opensearch.action.ActionResponse multiSearchResponse,
                                      MultiQueryRequest parameter) {

        Map<String, ActionResponse> queryVsQueryResponseMap = Maps.newHashMap();
        int queryCounter = 0;
        List<String> queryKeys = Lists.newArrayList();
        List<ActionRequest> requests = Lists.newArrayList();
        for (Map.Entry<String, ActionRequest> entry : getParameter().getRequests()
                .entrySet()) {
            queryKeys.add(entry.getKey());
            requests.add(entry.getValue());
        }
        for (MultiSearchResponse.Item item : ((MultiSearchResponse) multiSearchResponse).getResponses()) {
            Action action = null;
            ActionRequest request = requests.get(queryCounter);
            try {
                action = analyticsLoader.getAction(request);
            } catch (Exception e) {
                log.error("Error occurred while executing multiQuery request : {}", e);
            }
            if (null == action) {
                throw FoxtrotExceptions.queryCreationException(request, null);
            }
            String key = queryKeys.get(queryCounter++);
            ActionResponse response = action.getResponse(item.getResponse(), request);
            queryVsQueryResponseMap.put(key, response);
        }
        return new MultiQueryResponse(queryVsQueryResponseMap);
    }

    private void createActions(final MultiQueryRequest multiQueryRequest) {
        if (Utils.hasTemporalFilters(multiQueryRequest.getFilters())) {
            val offendingRequests = multiQueryRequest.getRequests().entrySet().stream()
                    .filter(entry -> Utils.hasTemporalFilters(entry.getValue().getFilters()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(offendingRequests)) {
                throw FoxtrotExceptions.createMalformedQueryException(
                        multiQueryRequest,
                        Collections.singletonList(
                                "Temporal filters passed in multi query as well as children: " + offendingRequests));
            }
        }

        for (Map.Entry<String, ActionRequest> entry : multiQueryRequest.getRequests().entrySet()) {
            ActionRequest request = entry.getValue();
            Action action;
            if (requestActionMap.get(request) != null) {
                action = requestActionMap.get(request);
            } else {
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
        for (Map.Entry<String, ActionRequest> entry : multiQueryRequest.getRequests().entrySet()) {
            if (null == entry.getValue()) {
                log.warn("Empty response for query: {}", entry.getKey());
                continue;
            }
            String result = actionInterface.invoke(requestActionMap.get(entry.getValue()), entry.getValue());
            if (!Strings.isNullOrEmpty(result)) {
                results.add(result);
            }
        }
        return String.join("-", results);
    }
}