package com.flipkart.foxtrot.core.querystore.actions;

<<<<<<< HEAD
import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.google.common.collect.Lists;
=======
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.*;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.alerts.EmailConfig;
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
import org.elasticsearch.action.ActionRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

>>>>>>> phonepe-develop
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.ActionRequestBuilder;
=======
>>>>>>> phonepe-develop

/***
 Created by mudit.g on Jan, 2019
 ***/
<<<<<<< HEAD
@AnalyticsProvider(opcode = "multi_time_query", request = MultiTimeQueryRequest.class, response =
        MultiTimeQueryResponse.class, cacheable = false)
public class MultiTimeQueryAction extends Action<MultiTimeQueryRequest> {

=======
@AnalyticsProvider(opcode = "multi_time_query", request = MultiTimeQueryRequest.class, response = MultiTimeQueryResponse.class, cacheable = false)
public class MultiTimeQueryAction extends Action<MultiTimeQueryRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTimeQueryAction.class);
>>>>>>> phonepe-develop
    private AnalyticsLoader analyticsLoader;
    private Action action;
    private MultiQueryRequest multiQueryRequest;

<<<<<<< HEAD
    public MultiTimeQueryAction(MultiTimeQueryRequest parameter, String cacheToken, AnalyticsLoader analyticsLoader) {
        super(parameter, cacheToken, analyticsLoader);
=======
    public MultiTimeQueryAction(MultiTimeQueryRequest parameter, TableMetadataManager tableMetadataManager, DataStore dataStore,
                                QueryStore queryStore, ElasticsearchConnection connection, String cacheToken,
                                CacheManager cacheManager, ObjectMapper objectMapper, EmailConfig emailConfig,
                                AnalyticsLoader analyticsLoader) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager,
                objectMapper, emailConfig);
>>>>>>> phonepe-develop
        this.analyticsLoader = analyticsLoader;
    }

    @Override
    public void preprocess() {
        MultiTimeQueryRequest multiTimeQueryRequest = getParameter();
        int sampleSize;
<<<<<<< HEAD

        if (multiTimeQueryRequest.getActionRequest() == null && CollectionUtils.isEmpty(
                multiTimeQueryRequest.getFilters())) {
            throw FoxtrotExceptions.createBadRequestException("multi_time_query",
                    "No Between Filter found in actionRequest " +
                            "multiQueryRequest : " + multiQueryRequest.toString());
        }

        if (CollectionUtils.isEmpty(multiTimeQueryRequest.getActionRequest()
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
        if (!optionalBetweenFilter.isPresent()) {
            throw FoxtrotExceptions.createBadRequestException("multi_time_query",
                    "No Between Filter found in actionRequest " +
                            "multiQueryRequest : " + multiQueryRequest.toString());
        }
        BetweenFilter betweenFilter = (BetweenFilter) optionalBetweenFilter.get();

        if (multiTimeQueryRequest.getSampleSize() != 0) {
            sampleSize = multiTimeQueryRequest.getSampleSize();
        } else if (multiTimeQueryRequest.getSkipDuration()
                .toHours() > TimeUnit.DAYS.toHours(1)) {
            sampleSize = (int) (30 / (multiTimeQueryRequest.getSkipDuration()
                    .toDays()));
        } else {
            sampleSize = (int) (24 / (multiTimeQueryRequest.getSkipDuration()
                    .toHours()));
        }
        multiQueryRequest = createMultiQueryRequests(sampleSize, betweenFilter);
        action = analyticsLoader.getAction(multiQueryRequest);
        if (null == action) {
            throw FoxtrotExceptions.queryCreationException(multiTimeQueryRequest, null);
=======
        BetweenFilter betweenFilter = (BetweenFilter) multiTimeQueryRequest.getActionRequest().getFilters().stream()
                .filter(filter -> filter instanceof BetweenFilter)
                .findFirst()
                .get();
        if (betweenFilter == null) {
            throw new RuntimeException("No Between Filter found in actionRequest multiQueryRequest : " + multiQueryRequest.toString());
        }
        if (multiTimeQueryRequest.getSampleSize() != 0) {
            sampleSize = multiTimeQueryRequest.getSampleSize();
        } else if (multiTimeQueryRequest.getSkipDuration().toHours() > 24) {
            sampleSize = (int) (30/(multiTimeQueryRequest.getSkipDuration().toDays()));
        } else {
            sampleSize = (int) (24/(multiTimeQueryRequest.getSkipDuration().toHours()));
        }
        if (multiTimeQueryRequest.getFilters() != null && multiTimeQueryRequest.getActionRequest() != null
                && multiTimeQueryRequest.getActionRequest().getFilters() != null) {
            multiTimeQueryRequest.getActionRequest().getFilters().addAll(multiTimeQueryRequest.getFilters());
        } else if (multiTimeQueryRequest.getFilters() != null && multiTimeQueryRequest.getActionRequest() != null
                && multiTimeQueryRequest.getActionRequest().getFilters() == null) {
            multiTimeQueryRequest.getActionRequest().setFilters(multiTimeQueryRequest.getFilters());
        }
        multiQueryRequest = createMultiQueryRequests(sampleSize, betweenFilter);
        try {
            action = analyticsLoader.getAction(multiQueryRequest);
        } catch (Exception e) {
            throw new RuntimeException("No action found for multiQueryRequest : " + multiQueryRequest.toString());
        }
        if(null == action) {
            throw new RuntimeException("No action found for multiQueryRequest : " + multiQueryRequest.toString());
>>>>>>> phonepe-develop
        }
        action.preprocess();
    }

    @Override
<<<<<<< HEAD
    public void validateImpl(MultiTimeQueryRequest parameter, String email) {
        List<String> validationErrors = new ArrayList<>();
        if (parameter.getActionRequest() == null) {
            validationErrors.add("action request cannot be null or empty");
        }
        if (parameter.getSkipDuration() == null) {
            validationErrors.add("skip duration cannot be null or empty");
        }
        if (com.collections.CollectionUtils.isNotEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
        action.validateImpl(multiQueryRequest, email);
=======
    public String getMetricKey() {
        return action.getMetricKey();
>>>>>>> phonepe-develop
    }

    @Override
    public String getRequestCacheKey() {
        return action.getRequestCacheKey();
    }

    @Override
<<<<<<< HEAD
    public ActionResponse execute(MultiTimeQueryRequest parameter) {
        MultiTimeQueryResponse multiTimeQueryResponse = new MultiTimeQueryResponse();
        multiTimeQueryResponse.setResponses(((MultiQueryResponse) action.execute(multiQueryRequest)).getResponses());
        return multiTimeQueryResponse;
    }

    @Override
    public String getMetricKey() {
        return action.getMetricKey();
    }

    @Override
    public ActionRequestBuilder getRequestBuilder(MultiTimeQueryRequest parameter) {
=======
    public void validateImpl(MultiTimeQueryRequest parameter) throws MalformedQueryException {
        List<String> validationErrors = new ArrayList<>();
        if(parameter.getActionRequest() == null) {
            validationErrors.add("action request cannot be null or empty");
        }
        if(parameter.getSkipDuration() == null) {
            validationErrors.add("skip duration cannot be null or empty");
        }
        if(!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
        action.validateImpl(multiQueryRequest);
    }

    @Override
    public ActionResponse execute(MultiTimeQueryRequest parameter) throws FoxtrotException {
        return action.execute(multiQueryRequest);
    }

    @Override
    public ActionRequestBuilder getRequestBuilder(MultiTimeQueryRequest parameter) throws FoxtrotException {
>>>>>>> phonepe-develop
        return action.getRequestBuilder(multiQueryRequest);
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse multiSearchResponse,
<<<<<<< HEAD
            MultiTimeQueryRequest parameter) {
        MultiQueryResponse multiQueryResponse = (MultiQueryResponse) action.getResponse(multiSearchResponse,
                multiQueryRequest);
=======
                                      MultiTimeQueryRequest parameter) throws FoxtrotException {
        MultiQueryResponse multiQueryResponse = (MultiQueryResponse)action.getResponse(multiSearchResponse, multiQueryRequest);
>>>>>>> phonepe-develop
        return new MultiTimeQueryResponse(multiQueryResponse.getResponses());
    }

    private MultiQueryRequest createMultiQueryRequests(int sampleSize, BetweenFilter betweenFilter) {
<<<<<<< HEAD
        MultiTimeQueryRequest multiTimeQueryRequest = getParameter();
        Map<String, ActionRequest> requests = new HashMap<>();
        long from = betweenFilter.getFrom()
                .longValue();
        long to = betweenFilter.getTo()
                .longValue();
        for (int itr = 0; itr < sampleSize; itr++) {
            List<Filter> filters = multiTimeQueryRequest.getActionRequest()
                    .getFilters();
            for (int i = 0; i < filters.size(); i++) {
                if (filters.get(i) instanceof BetweenFilter) {
                    BetweenFilter tempBetweenFilter = (BetweenFilter) filters.get(i);
                    BetweenFilter tempBetweenFilter1 = new BetweenFilter(tempBetweenFilter.getField(), from, to,
                            tempBetweenFilter.isFilterTemporal());
=======
        MultiTimeQueryRequest multiTimeQueryRequest = getParameter() ;
        Map<String, ActionRequest> requests = new HashMap<>();
        long from = betweenFilter.getFrom().longValue();
        long to = betweenFilter.getTo().longValue();
        for(int itr = 0; itr < sampleSize; itr++) {
            List<Filter> filters = multiTimeQueryRequest.getActionRequest().getFilters();
            for (int i = 0; i < filters.size(); i++) {
                if (filters.get(i) instanceof BetweenFilter) {
                    BetweenFilter tempBetweenFilter = (BetweenFilter) filters.get(i);
                    BetweenFilter tempBetweenFilter1 = new BetweenFilter(tempBetweenFilter.getField(),
                            from, to, tempBetweenFilter.isFilterTemporal());
>>>>>>> phonepe-develop
                    filters.set(i, tempBetweenFilter1);
                    break;
                }
            }
<<<<<<< HEAD
            multiTimeQueryRequest.getActionRequest()
                    .setFilters(filters);
            try {
                requests.put(Long.toString(from), (ActionRequest) multiTimeQueryRequest.getActionRequest()
                        .clone());
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(multiTimeQueryRequest.getActionRequest(), e);
            }

            from -= multiTimeQueryRequest.getSkipDuration()
                    .toMilliseconds();
            to -= multiTimeQueryRequest.getSkipDuration()
                    .toMilliseconds();
=======
            multiTimeQueryRequest.getActionRequest().setFilters(filters);
            try{
                requests.put(Long.toString(from), (ActionRequest) multiTimeQueryRequest.getActionRequest().clone());
            } catch (Exception e) {
                throw new RuntimeException("Error in cloning action request : " + multiTimeQueryRequest.getActionRequest().toString()
                        + " Error Message: " + e);
            }

            from -= multiTimeQueryRequest.getSkipDuration().toMilliseconds();
            to -= multiTimeQueryRequest.getSkipDuration().toMilliseconds();
>>>>>>> phonepe-develop
        }
        return new MultiQueryRequest(requests);
    }
}
