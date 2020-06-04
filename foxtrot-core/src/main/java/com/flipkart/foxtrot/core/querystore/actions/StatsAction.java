package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.stats.AnalyticsRequestFlags;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "stats", request = StatsRequest.class, response = StatsResponse.class, cacheable = true)
public class StatsAction extends Action<StatsRequest> {

    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    public StatsAction(StatsRequest parameter,
                       AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.elasticsearchTuningConfig = analyticsLoader.getElasticsearchTuningConfig();
    }

    private static StatsValue buildStatsValue(String field,
                                              Aggregations aggregations) {
        String metricKey = Utils.getExtendedStatsAggregationKey(field);
        String percentileMetricKey = Utils.getPercentileAggregationKey(field);

        // Build top level stats
        StatsValue statsValue = new StatsValue();
        statsValue.setStats(Utils.toStats(aggregations.getAsMap()
                .get(metricKey)));
        Percentiles internalPercentile = (Percentiles) aggregations.getAsMap()
                .get(percentileMetricKey);
        if (null != internalPercentile) {
            statsValue.setPercentiles(Utils.createPercentilesResponse(internalPercentile));
        }
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
        if (null != statsRequest.getFilters()) {
            for (Filter filter : statsRequest.getFilters()) {
                statsHashKey += 31 * filter.hashCode();
            }
        }

        if (!CollectionUtils.isNullOrEmpty(statsRequest.getNesting())) {
            for (String nestingKey : statsRequest.getNesting()) {
                statsHashKey += 31 * nestingKey.hashCode();
            }
        }

        return String.format("%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public void validateImpl(StatsRequest parameter) {
        List<String> validationErrors = Lists.newArrayList();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(StatsRequest parameter) {
        SearchRequest query = getRequestBuilder(parameter, Collections.emptyList());
        try {
            SearchResponse response = getConnection().getClient()
                    .search(query, RequestOptions.DEFAULT);
            return getResponse(response, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequest getRequestBuilder(StatsRequest parameter,
                                           List<Filter> extraFilters) {
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0)
                .timeout(new TimeValue(getGetQueryTimeout(), TimeUnit.MILLISECONDS))
                .query(ElasticsearchQueryUtils.translateFilter(parameter, extraFilters));

        AbstractAggregationBuilder percentiles = null;
        final String field = getParameter().getField();
        boolean isNumericField = Utils.isNumericField(getTableMetadataManager(), parameter.getTable(), field);
        final AbstractAggregationBuilder extendedStats;
        if (isNumericField) {
            if (!AnalyticsRequestFlags.hasFlag(parameter.getFlags(), AnalyticsRequestFlags.STATS_SKIP_PERCENTILES)) {
                percentiles = Utils.buildPercentileAggregation(field, getParameter().getPercentiles());
                sourceBuilder.aggregation(percentiles);
            }
            extendedStats = Utils.buildStatsAggregation(field, getParameter().getStats());
        } else {
            extendedStats = Utils.buildStatsAggregation(field, Collections.singleton(Stat.COUNT));
        }
        sourceBuilder.aggregation(extendedStats);
        if (!CollectionUtils.isNullOrEmpty(getParameter().getNesting())) {
            final HashSet<AggregationBuilder> subAggregations = new HashSet<>();
            subAggregations.add(extendedStats);
            if (null != percentiles) {
                subAggregations.add(percentiles);
            }
            sourceBuilder.aggregation(Utils.buildTermsAggregation(getParameter().getNesting()
                    .stream()
                    .map(x -> new ResultSort(x, ResultSort.Order.asc))
                    .collect(Collectors.toList()), subAggregations, elasticsearchTuningConfig.getAggregationSize()));
        }
        return new SearchRequest(ElasticsearchUtils.getIndices(parameter.getTable(), parameter)).indicesOptions(
                Utils.indicesOptions())
                .source(sourceBuilder);
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response,
                                      StatsRequest parameter) {
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        if (aggregations != null) {
            return buildResponse(parameter, aggregations);
        }
        return null;
    }

    private StatsResponse buildResponse(StatsRequest request,
                                        Aggregations aggregations) {
        // First build root level stats value
        StatsValue statsValue = buildStatsValue(request.getField(), aggregations);

        // Now build nested stats if present
        List<BucketResponse<StatsValue>> buckets = null;
        if (!CollectionUtils.isNullOrEmpty(request.getNesting())) {
            buckets = buildNestedStats(request.getNesting(), aggregations);
        }

        StatsResponse statsResponse = new StatsResponse();
        statsResponse.setResult(statsValue);
        statsResponse.setBuckets(buckets);

        return statsResponse;
    }

    private List<BucketResponse<StatsValue>> buildNestedStats(List<String> nesting,
                                                              Aggregations aggregations) {
        final String field = nesting.get(0);
        final List<String> remainingFields = (nesting.size() > 1)
                                             ? nesting.subList(1, nesting.size())
                                             : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        List<BucketResponse<StatsValue>> bucketResponses = Lists.newArrayList();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            BucketResponse<StatsValue> bucketResponse = new BucketResponse<>();
            bucketResponse.setKey(String.valueOf(bucket.getKey()));
            if (nesting.size() == 1) {
                bucketResponse.setResult(buildStatsValue(getParameter().getField(), bucket.getAggregations()));
            } else {
                bucketResponse.setBuckets(buildNestedStats(remainingFields, bucket.getAggregations()));
            }
            bucketResponses.add(bucketResponse);
        }
        return bucketResponses;
    }

}

