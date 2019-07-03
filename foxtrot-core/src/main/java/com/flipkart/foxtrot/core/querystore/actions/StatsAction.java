package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "stats", request = StatsRequest.class, response = StatsResponse.class, cacheable = false)
public class StatsAction extends Action<StatsRequest> {

    public StatsAction(StatsRequest parameter, String cacheToken, AnalyticsLoader analyticsLoader) {
        super(parameter, cacheToken, analyticsLoader);
    }

    private static StatsValue buildStatsValue(String field, Aggregations aggregations) {
        String metricKey = Utils.getExtendedStatsAggregationKey(field);
        String percentileMetricKey = Utils.getPercentileAggregationKey(field);

        // Build top level stats
        StatsValue statsValue = new StatsValue();
        statsValue.setStats(Utils.toStats(aggregations.getAsMap()
                                                  .get(metricKey)));
        Percentiles internalPercentile = (Percentiles)aggregations.getAsMap()
                .get(percentileMetricKey);
        statsValue.setPercentiles(Utils.createPercentilesResponse(internalPercentile));
        return statsValue;
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
        long statsHashKey = 0L;
        StatsRequest statsRequest = getParameter();
        if(null != statsRequest.getFilters()) {
            for(Filter filter : statsRequest.getFilters()) {
                statsHashKey += 31 * filter.hashCode();
            }
        }

        if(! CollectionUtils.isNullOrEmpty(statsRequest.getNesting())) {
            for(String nestingKey : statsRequest.getNesting()) {
                statsHashKey += 31 * nestingKey.hashCode();
            }
        }

        return String.format("%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public void validateImpl(StatsRequest parameter, String email) {
        List<String> validationErrors = Lists.newArrayList();
        if(CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if(CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if(! CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(StatsRequest parameter) {
        SearchRequestBuilder searchRequestBuilder = getRequestBuilder(parameter);
        try {
            SearchResponse response = searchRequestBuilder.execute()
                    .actionGet(getGetQueryTimeout());
            return getResponse(response, parameter);
        } catch(ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequestBuilder getRequestBuilder(StatsRequest parameter) {
        SearchRequestBuilder searchRequestBuilder;
        try {
            searchRequestBuilder = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator().genFilter(parameter.getFilters()))
                    .setSize(QUERY_SIZE);

            AbstractAggregationBuilder percentiles = Utils.buildPercentileAggregation(getParameter().getField(),
                                                                                      getParameter().getPercentiles());
            AbstractAggregationBuilder extendedStats = Utils.buildStatsAggregation(getParameter().getField(),
                                                                                   getParameter().getStats());
            searchRequestBuilder.addAggregation(percentiles);
            searchRequestBuilder.addAggregation(extendedStats);

            if(! CollectionUtils.isNullOrEmpty(getParameter().getNesting())) {
                searchRequestBuilder.addAggregation(Utils.buildTermsAggregation(getParameter().getNesting()
                                                                                        .stream()
                                                                                        .map(x -> new ResultSort(x,
                                                                                                                 ResultSort.Order.asc))
                                                                                        .collect(Collectors.toList()),
                                                                                Sets.newHashSet(percentiles,
                                                                                                extendedStats)));
            }
        } catch(Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        return searchRequestBuilder;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, StatsRequest parameter) {
        Aggregations aggregations = ((SearchResponse)response).getAggregations();
        if(aggregations != null) {
            return buildResponse(parameter, aggregations);
        }
        return null;
    }

    private StatsResponse buildResponse(StatsRequest request, Aggregations aggregations) {
        // First build root level stats value
        StatsValue statsValue = buildStatsValue(request.getField(), aggregations);

        // Now build nested stats if present
        List<BucketResponse<StatsValue>> buckets = null;
        if(! CollectionUtils.isNullOrEmpty(request.getNesting())) {
            buckets = buildNestedStats(request.getNesting(), aggregations);
        }

        StatsResponse statsResponse = new StatsResponse();
        statsResponse.setResult(statsValue);
        statsResponse.setBuckets(buckets);

        return statsResponse;
    }

    private List<BucketResponse<StatsValue>> buildNestedStats(List<String> nesting, Aggregations aggregations) {
        final String field = nesting.get(0);
        final List<String> remainingFields = (nesting.size() > 1) ? nesting.subList(1, nesting.size()) :
                                             new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        List<BucketResponse<StatsValue>> bucketResponses = Lists.newArrayList();
        for(Terms.Bucket bucket : terms.getBuckets()) {
            BucketResponse<StatsValue> bucketResponse = new BucketResponse<>();
            bucketResponse.setKey(String.valueOf(bucket.getKey()));
            if(nesting.size() == 1) {
                bucketResponse.setResult(buildStatsValue(getParameter().getField(), bucket.getAggregations()));
            } else {
                bucketResponse.setBuckets(buildNestedStats(remainingFields, bucket.getAggregations()));
            }
            bucketResponses.add(bucketResponse);
        }
        return bucketResponses;
    }

}

