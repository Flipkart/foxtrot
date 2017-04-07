package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendValue;
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
import io.dropwizard.util.Duration;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "statstrend", request = StatsTrendRequest.class, response = StatsTrendResponse.class, cacheable = false)
public class StatsTrendAction extends Action<StatsTrendRequest> {

    public StatsTrendAction(StatsTrendRequest parameter,
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
        StatsTrendRequest statsRequest = getParameter();
        long hashKey = 0L;
        if (statsRequest.getFilters() != null) {
            for (Filter filter : statsRequest.getFilters()) {
                hashKey += 31 * filter.hashCode();
            }
        }

        if (!CollectionUtils.isNullOrEmpty(statsRequest.getNesting())) {
            for (String field : statsRequest.getNesting()) {
                hashKey += 31 * field.hashCode();
            }
        }

        hashKey += 31 * statsRequest.getPeriod().name().hashCode();
        hashKey += 31 * statsRequest.getTimestamp().hashCode();
        hashKey += 31 * (statsRequest.getField() != null ? statsRequest.getField().hashCode() : "FIELD".hashCode());
        return String.format("stats-trend-%s-%s-%s-%d", statsRequest.getTable(),
                statsRequest.getField(), statsRequest.getPeriod(), hashKey);
    }

    @Override
    public void validateImpl(StatsTrendRequest parameter) throws MalformedQueryException {
        List<String> validationErrors = Lists.newArrayList();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (CollectionUtils.isNullOrEmpty(parameter.getTimestamp())) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if (parameter.getCombiner() == null) {
            validationErrors.add(String.format("specify filter combiner (%s)", StringUtils.join(FilterCombinerType.values())));
        }
        if (parameter.getPeriod() == null) {
            validationErrors.add(String.format("specify time period (%s)", StringUtils.join(Period.values())));
        }

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(StatsTrendRequest parameter) throws FoxtrotException {
        SearchRequestBuilder searchRequestBuilder;
        try {
            AbstractAggregationBuilder aggregation = buildAggregation(parameter);
            searchRequestBuilder = getConnection().getClient().prepareSearch(
                    ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator(parameter.getCombiner()).genFilter(parameter.getFilters()))
                    .setSize(0)
                    .addAggregation(aggregation);
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }

        try {
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            Aggregations aggregations = response.getAggregations();
            if (aggregations != null) {
                return buildResponse(parameter, aggregations);
            }
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
        return null;
    }

    private AbstractAggregationBuilder buildAggregation(StatsTrendRequest request) {
        DateHistogramInterval interval = Utils.getHistogramInterval(request.getPeriod());
        AbstractAggregationBuilder dateHistogramBuilder = Utils.buildDateHistogramAggregation(request.getTimestamp(), interval)
                .subAggregation(Utils.buildExtendedStatsAggregation(request.getField()))
                .subAggregation(Utils.buildPercentileAggregation(request.getField()));

        if (CollectionUtils.isNullOrEmpty(getParameter().getNesting())) {
            return dateHistogramBuilder;
        }

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
        termsBuilder.subAggregation(dateHistogramBuilder);
        return rootBuilder;
    }

    private StatsTrendResponse buildResponse(StatsTrendRequest request, Aggregations aggregations) {
        StatsTrendResponse response = new StatsTrendResponse();

        if (CollectionUtils.isNullOrEmpty(request.getNesting())) {
            List<StatsTrendValue> trends = buildStatsTrendValue(request.getField(), aggregations);
            response.setResult(trends);
        } else {
            List<BucketResponse<List<StatsTrendValue>>> buckets = buildNestedTrendStats(request.getNesting(), aggregations);
            response.setBuckets(buckets);
        }
        return response;
    }

    private List<BucketResponse<List<StatsTrendValue>>> buildNestedTrendStats(List<String> nesting,
                                                                              Aggregations aggregations) {
        final String field = nesting.get(0);
        final List<String> remainingFields = (nesting.size() > 1) ? nesting.subList(1, nesting.size())
                : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        List<BucketResponse<List<StatsTrendValue>>> bucketResponses = Lists.newArrayList();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            BucketResponse<List<StatsTrendValue>> bucketResponse = new BucketResponse<>();
            bucketResponse.setKey(String.valueOf(bucket.getKey()));
            if (nesting.size() == 1) {
                bucketResponse.setResult(buildStatsTrendValue(getParameter().getField(), bucket.getAggregations()));
            } else {
                bucketResponse.setBuckets(buildNestedTrendStats(remainingFields, bucket.getAggregations()));
            }
            bucketResponses.add(bucketResponse);
        }
        return bucketResponses;
    }

    private List<StatsTrendValue> buildStatsTrendValue(String field, Aggregations aggregations) {
        String dateHistogramKey = Utils.getDateHistogramKey(getParameter().getTimestamp());
        Histogram dateHistogram = aggregations.get(dateHistogramKey);
        Collection<? extends Histogram.Bucket> buckets = dateHistogram.getBuckets();

        String metricKey = Utils.getExtendedStatsAggregationKey(field);
        String percentileMetricKey = Utils.getPercentileAggregationKey(field);

        List<StatsTrendValue> statsValueList = Lists.newArrayList();
        for (Histogram.Bucket bucket : buckets) {
            StatsTrendValue statsTrendValue = new StatsTrendValue();
            DateTime key = (DateTime) bucket.getKey();
            statsTrendValue.setPeriod(key.getMillis());

            InternalExtendedStats extendedStats = InternalExtendedStats.class.cast(bucket.getAggregations().getAsMap().get(metricKey));
            statsTrendValue.setStats(Utils.createExtendedStatsResponse(extendedStats));
            Percentiles internalPercentile = Percentiles.class.cast(bucket.getAggregations().getAsMap().get(percentileMetricKey));
            statsTrendValue.setPercentiles(Utils.createPercentilesResponse(internalPercentile));
            statsValueList.add(statsTrendValue);
        }
        return statsValueList;
    }


    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

}

