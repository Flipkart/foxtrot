package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "stats", request = StatsRequest.class, response = StatsResponse.class, cacheable = false)
public class StatsAction extends Action<StatsRequest> {

    public StatsAction(StatsRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken,
                       CacheManager cacheManager) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager);
    }

    @Override
    protected void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    protected String getRequestCacheKey() {
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

        statsHashKey += 31 * statsRequest.getCombiner().hashCode();
        return String.format("%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public void validateImpl(StatsRequest parameter) throws MalformedQueryException {
        List<String> validationErrors = Lists.newArrayList();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (parameter.getCombiner() == null) {
            validationErrors.add(String.format("specify filter combiner (%s)", StringUtils.join(FilterCombinerType.values())));
        }

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(StatsRequest parameter) throws FoxtrotException {
        SearchRequestBuilder searchRequestBuilder;
        try {
            searchRequestBuilder = getConnection().getClient().prepareSearch(
                    ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator(parameter.getCombiner()).genFilter(parameter.getFilters()))
                    .setSize(0);

            AbstractAggregationBuilder percentiles = Utils.buildPercentileAggregation(getParameter().getField());
            AbstractAggregationBuilder extendedStats = Utils.buildExtendedStatsAggregation(getParameter().getField());

            searchRequestBuilder.addAggregation(percentiles);
            searchRequestBuilder.addAggregation(extendedStats);

            if (!CollectionUtils.isNullOrEmpty(getParameter().getNesting())) {
                TermsBuilder rootBuilder = null;
                TermsBuilder termsBuilder = null;
                for (String field : getParameter().getNesting()) {
                    if (null == termsBuilder) {
                        termsBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field)).field(field);
                    } else {
                        TermsBuilder tempBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field)).field(field);
                        termsBuilder.subAggregation(tempBuilder);
                        termsBuilder = tempBuilder;
                    }
                    termsBuilder.size(0);
                    if (null == rootBuilder) {
                        rootBuilder = termsBuilder;
                    }
                }
                termsBuilder.subAggregation(percentiles);
                termsBuilder.subAggregation(extendedStats);
                searchRequestBuilder.addAggregation(rootBuilder);
            }
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        try {
            Aggregations aggregations = searchRequestBuilder.execute().actionGet().getAggregations();
            if (aggregations != null) {
                return buildResponse(parameter, aggregations);
            }
            return null;
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    private StatsResponse buildResponse(StatsRequest request, Aggregations aggregations) {
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

    private List<BucketResponse<StatsValue>> buildNestedStats(List<String> nesting, Aggregations aggregations) {
        final String field = nesting.get(0);
        final List<String> remainingFields = (nesting.size() > 1) ? nesting.subList(1, nesting.size())
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

    private static StatsValue buildStatsValue(String field, Aggregations aggregations) {
        String metricKey = Utils.getExtendedStatsAggregationKey(field);
        String percentileMetricKey = Utils.getPercentileAggregationKey(field);

        // Build top level stats
        StatsValue statsValue = new StatsValue();
        InternalExtendedStats extendedStats = InternalExtendedStats.class.cast(aggregations.getAsMap().get(metricKey));
        statsValue.setStats(Utils.createExtendedStatsResponse(extendedStats));
        Percentiles internalPercentile = Percentiles.class.cast(aggregations.getAsMap().get(percentileMetricKey));
        statsValue.setPercentiles(Utils.createPercentilesResponse(internalPercentile));
        return statsValue;
    }

}

