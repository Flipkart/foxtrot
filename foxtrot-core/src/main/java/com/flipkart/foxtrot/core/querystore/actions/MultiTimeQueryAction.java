package com.flipkart.foxtrot.core.querystore.actions;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.*;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequestBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;

/***
 Created by mudit.g on Jan, 2019
 ***/
@AnalyticsProvider(opcode = "multi_time_query", request = MultiTimeQueryRequest.class, response = MultiTimeQueryResponse.class, cacheable
        = false)
public class MultiTimeQueryAction extends Action<MultiTimeQueryRequest> {

    private AnalyticsLoader analyticsLoader;
    private Action action;
    private MultiQueryRequest multiQueryRequest;

    public MultiTimeQueryAction(MultiTimeQueryRequest parameter, String cacheToken, AnalyticsLoader analyticsLoader) {
        super(parameter, cacheToken, analyticsLoader);
        this.analyticsLoader = analyticsLoader;
    }

    @Override
    public void preprocess() {
        MultiTimeQueryRequest multiTimeQueryRequest = getParameter();
        int sampleSize;

        if(multiTimeQueryRequest.getActionRequest() == null && CollectionUtils.isEmpty(multiTimeQueryRequest.getFilters())) {
            throw FoxtrotExceptions.createBadRequestException("multi_time_query",
                                                              "No Between Filter found in actionRequest multiQueryRequest : " +
                                                              multiQueryRequest.toString()
                                                             );
        }

        if(CollectionUtils.isEmpty(multiTimeQueryRequest.getActionRequest()
                                           .getFilters())) {
            multiTimeQueryRequest.getActionRequest()
                    .setFilters(Lists.newArrayList());
        }
        multiTimeQueryRequest.getActionRequest()
                .getFilters()
                .addAll(multiTimeQueryRequest.getFilters());

        Optional<Filter> optionalBetweenFilter = multiTimeQueryRequest.getActionRequest()
                .getFilters()
                .stream()
                .filter(filter -> filter instanceof BetweenFilter)
                .findFirst();
        if(!optionalBetweenFilter.isPresent()) {
            throw FoxtrotExceptions.createBadRequestException("multi_time_query",
                                                              "No Between Filter found in actionRequest multiQueryRequest : " +
                                                              multiQueryRequest.toString()
                                                             );
        }
        BetweenFilter betweenFilter = (BetweenFilter)optionalBetweenFilter.get();

        if(multiTimeQueryRequest.getSampleSize() != 0) {
            sampleSize = multiTimeQueryRequest.getSampleSize();
        } else if(multiTimeQueryRequest.getSkipDuration()
                          .toHours() > TimeUnit.DAYS.toHours(1)) {
            sampleSize = (int)(30 / (multiTimeQueryRequest.getSkipDuration()
                    .toDays()));
        } else {
            sampleSize = (int)(24 / (multiTimeQueryRequest.getSkipDuration()
                    .toHours()));
        }
        multiQueryRequest = createMultiQueryRequests(sampleSize, betweenFilter);
        action = analyticsLoader.getAction(multiQueryRequest);
        if(null == action) {
            throw FoxtrotExceptions.queryCreationException(multiTimeQueryRequest, null);
        }
        action.preprocess();
    }

    @Override
    public String getMetricKey() {
        return action.getMetricKey();
    }

    @Override
    public String getRequestCacheKey() {
        return action.getRequestCacheKey();
    }

    @Override
    public void validateImpl(MultiTimeQueryRequest parameter, String email) {
        List<String> validationErrors = new ArrayList<>();
        if(parameter.getActionRequest() == null) {
            validationErrors.add("action request cannot be null or empty");
        }
        if(parameter.getSkipDuration() == null) {
            validationErrors.add("skip duration cannot be null or empty");
        }
        if(com.collections.CollectionUtils.isNotEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
        action.validateImpl(multiQueryRequest, email);
    }

    @Override
    public ActionResponse execute(MultiTimeQueryRequest parameter) {
        MultiTimeQueryResponse multiTimeQueryResponse = new MultiTimeQueryResponse();
        multiTimeQueryResponse.setResponses(((MultiQueryResponse)action.execute(multiQueryRequest)).getResponses());
        return multiTimeQueryResponse;
    }

    @Override
    public ActionRequestBuilder getRequestBuilder(MultiTimeQueryRequest parameter) {
        return action.getRequestBuilder(multiQueryRequest);
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse multiSearchResponse, MultiTimeQueryRequest parameter) {
        MultiQueryResponse multiQueryResponse = (MultiQueryResponse)action.getResponse(multiSearchResponse, multiQueryRequest);
        return new MultiTimeQueryResponse(multiQueryResponse.getResponses());
    }

    private MultiQueryRequest createMultiQueryRequests(int sampleSize, BetweenFilter betweenFilter) {
        MultiTimeQueryRequest multiTimeQueryRequest = getParameter();
        Map<String, ActionRequest> requests = new HashMap<>();
        long from = betweenFilter.getFrom()
                .longValue();
        long to = betweenFilter.getTo()
                .longValue();
        for(int itr = 0; itr < sampleSize; itr++) {
            List<Filter> filters = multiTimeQueryRequest.getActionRequest()
                    .getFilters();
            for(int i = 0; i < filters.size(); i++) {
                if(filters.get(i) instanceof BetweenFilter) {
                    BetweenFilter tempBetweenFilter = (BetweenFilter)filters.get(i);
                    BetweenFilter tempBetweenFilter1 = new BetweenFilter(tempBetweenFilter.getField(), from, to,
                                                                         tempBetweenFilter.isFilterTemporal()
                    );
                    filters.set(i, tempBetweenFilter1);
                    break;
                }
            }
            multiTimeQueryRequest.getActionRequest()
                    .setFilters(filters);
            try {
                requests.put(Long.toString(from), (ActionRequest)multiTimeQueryRequest.getActionRequest()
                        .clone());
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(multiTimeQueryRequest.getActionRequest(), e);
            }

            from -= multiTimeQueryRequest.getSkipDuration()
                    .toMilliseconds();
            to -= multiTimeQueryRequest.getSkipDuration()
                    .toMilliseconds();
        }
        return new MultiQueryRequest(requests);
    }
}
