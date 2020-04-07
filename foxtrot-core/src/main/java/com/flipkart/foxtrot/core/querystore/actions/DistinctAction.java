package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.config.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Sets;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

/**
 * Created by rishabh.goyal on 17/11/14.
 */

@AnalyticsProvider(opcode = "distinct", request = DistinctRequest.class, response = DistinctResponse.class, cacheable = true)
public class DistinctAction extends Action<DistinctRequest> {
    private static final Logger logger = LoggerFactory.getLogger(DistinctAction.class);

    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    public DistinctAction(DistinctRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.elasticsearchTuningConfig = analyticsLoader.getElasticsearchTuningConfig();
    }

    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public String getRequestCacheKey() {
        long filterHashKey = 0L;
        DistinctRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (int i = 0; i < query.getNesting()
                .size(); i++) {
            filterHashKey += 31 * query.getNesting()
                    .get(i)
                    .hashCode() * (i + 1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public void validateImpl(DistinctRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (CollectionUtils.isNullOrEmpty(parameter.getNesting())) {
            validationErrors.add("At least one nesting parameter is required");
        }
        else {
            for (ResultSort resultSort : com.collections.CollectionUtils.nullSafeList(parameter.getNesting())) {

                if (CollectionUtils.isNullOrEmpty(resultSort.getField())) {
                    validationErrors.add("nested parameter cannot have null name");
                }
                if (resultSort.getOrder() == null) {
                    validationErrors.add("nested parameter cannot have null sorting order");
                }

            }
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(DistinctRequest request) {
        SearchRequest query;
        try {
            query = getRequestBuilder(request);
        }
        catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(request, e);
        }

        try {
            SearchResponse response = getConnection()
                    .getClient()
                    .search(query);

            return getResponse(response, getParameter());
        }
        catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(request, e);
        }
    }

    @Override
    public SearchRequest getRequestBuilder(DistinctRequest request) {
        try {
            return new SearchRequest(ElasticsearchUtils.getIndices(request.getTable(), request))
                    .indicesOptions(Utils.indicesOptions())
                    .source(new SearchSourceBuilder()
                                    .query(new ElasticSearchQueryGenerator().genFilter(request.getFilters()))
                                    .size(QUERY_SIZE)
                                    .aggregation(Utils.buildTermsAggregation(
                                            request.getNesting(), Sets.newHashSet(), elasticsearchTuningConfig.getAggregationSize()))
                                    .timeout(new TimeValue(getGetQueryTimeout(), TimeUnit.MILLISECONDS)));

        }
        catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(request, e);
        }
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, DistinctRequest parameter) {
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        // Check if any aggregation is present or not
        if (aggregations == null) {
            logger.error("Null response for Group. Request : {}", parameter);
            return new DistinctResponse(new ArrayList<>(), new ArrayList<>());
        }
        return getDistinctResponse(parameter, aggregations);
    }

    private DistinctResponse getDistinctResponse(DistinctRequest request, Aggregations aggregations) {
        DistinctResponse response = new DistinctResponse();
        List<String> headerList = request.getNesting()
                .stream()
                .map(ResultSort::getField)
                .collect(Collectors.toList());
        response.setHeaders(headerList);

        List<List<String>> responseList = new ArrayList<>();
        flatten(null, headerList, responseList, aggregations);
        response.setResult(responseList);
        return response;
    }

    private void flatten(
            String parentKey,
            List<String> fields,
            List<List<String>> responseList,
            Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1)
                                             ? fields.subList(1, fields.size())
                                             : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                responseList.add(getValueList(parentKey, String.valueOf(bucket.getKey())));
            }
            else {
                flatten(getProperKey(parentKey, String.valueOf(bucket.getKey())),
                        remainingFields,
                        responseList,
                        bucket.getAggregations());
            }
        }
    }

    private String getProperKey(String parentKey, String currentKey) {
        return parentKey == null
               ? currentKey
               : parentKey + Constants.SEPARATOR + currentKey;
    }

    private List<String> getValueList(String parentKey, String currentKey) {
        String finalValue = getProperKey(parentKey, currentKey);
        String[] valuesList = finalValue.split(Constants.SEPARATOR);
        return Arrays.asList(valuesList);
    }
}
