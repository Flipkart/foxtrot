package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.*;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.stats.*;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.util.Duration;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.joda.time.DateTime;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "statstrend", request = StatsTrendRequest.class, response = StatsTrendResponse.class, cacheable = false)
public class StatsTrendAction extends Action<StatsTrendRequest> {

    public StatsTrendAction(StatsTrendRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
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
        StatsTrendRequest statsRequest = getParameter();
        long hashKey = 0L;
        if(statsRequest.getFilters() != null) {
            for(Filter filter : statsRequest.getFilters()) {
                hashKey += 31 * filter.hashCode();
            }
        }

        if(!CollectionUtils.isNullOrEmpty(statsRequest.getNesting())) {
            for(String field : statsRequest.getNesting()) {
                hashKey += 31 * field.hashCode();
            }
        }

        hashKey += 31 * statsRequest.getPeriod()
                .name()
                .hashCode();
        hashKey += 31 * statsRequest.getTimestamp()
                .hashCode();
        hashKey += 31 * (statsRequest.getField() != null ? statsRequest.getField()
                .hashCode() : "FIELD".hashCode());
        return String.format("stats-trend-%s-%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsRequest.getPeriod(),
                             hashKey
                            );
    }

    @Override
    public void validateImpl(StatsTrendRequest parameter) {
        List<String> validationErrors = Lists.newArrayList();
        if(CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if(CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if(CollectionUtils.isNullOrEmpty(parameter.getTimestamp())) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if(parameter.getPeriod() == null) {
            validationErrors.add(String.format("specify time period (%s)", StringUtils.join(Period.values())));
        }
        if(!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(StatsTrendRequest parameter) {
        SearchRequestBuilder searchRequestBuilder = getRequestBuilder(parameter);

        try {
            SearchResponse response = searchRequestBuilder.execute()
                    .actionGet(getGetQueryTimeout());
            return getResponse(response, parameter);
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequestBuilder getRequestBuilder(StatsTrendRequest parameter) {
        SearchRequestBuilder searchRequestBuilder;
        try {
            final String table = parameter.getTable();
            AbstractAggregationBuilder aggregation = buildAggregation(parameter, table);
            searchRequestBuilder = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(table, parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator().genFilter(parameter.getFilters()))
                    .setSize(0)
                    .addAggregation(aggregation);
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        return searchRequestBuilder;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, StatsTrendRequest parameter) {
        Aggregations aggregations = ((SearchResponse)response).getAggregations();
        if(aggregations != null) {
            return buildResponse(parameter, aggregations);
        }
        return null;
    }

    private AbstractAggregationBuilder buildAggregation(StatsTrendRequest request, String table) {
        final String field = request.getField();
        DateHistogramInterval interval = Utils.getHistogramInterval(request.getPeriod());
        AbstractAggregationBuilder dateHistogramBuilder = Utils.buildDateHistogramAggregation(request.getTimestamp(), interval);
        boolean isNumericField = Utils.isNumericField(getTableMetadataManager(), table, field);
        if(isNumericField) {
            dateHistogramBuilder
                    .subAggregation(Utils.buildStatsAggregation(field, getParameter().getStats()));
            if(!AnalyticsRequestFlags.hasFlag(request.getFlags(), AnalyticsRequestFlags.STATS_SKIP_PERCENTILES)) {
                dateHistogramBuilder.subAggregation(Utils.buildPercentileAggregation(
                        field, request.getPercentiles(), request.getCompression()));
            }
        }
        else {
            dateHistogramBuilder
                    .subAggregation(Utils.buildStatsAggregation(field, Collections.singleton(Stat.COUNT)));
        }

        if(CollectionUtils.isNullOrEmpty(getParameter().getNesting())) {
            return dateHistogramBuilder;
        }
        return Utils.buildTermsAggregation(getParameter().getNesting()
                                                   .stream()
                                                   .map(x -> new ResultSort(x, ResultSort.Order.asc))
                                                   .collect(Collectors.toList()), Sets.newHashSet(dateHistogramBuilder));
    }

    private StatsTrendResponse buildResponse(StatsTrendRequest request, Aggregations aggregations) {
        StatsTrendResponse response = new StatsTrendResponse();

        if(CollectionUtils.isNullOrEmpty(request.getNesting())) {
            List<StatsTrendValue> trends = buildStatsTrendValue(request.getField(), aggregations);
            response.setResult(trends);
        } else {
            List<BucketResponse<List<StatsTrendValue>>> buckets = buildNestedTrendStats(request.getNesting(), aggregations);
            response.setBuckets(buckets);
        }
        return response;
    }

    private List<BucketResponse<List<StatsTrendValue>>> buildNestedTrendStats(List<String> nesting, Aggregations aggregations) {
        final String field = nesting.get(0);
        final List<String> remainingFields = (nesting.size() > 1) ? nesting.subList(1, nesting.size()) : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        List<BucketResponse<List<StatsTrendValue>>> bucketResponses = Lists.newArrayList();
        for(Terms.Bucket bucket : terms.getBuckets()) {
            BucketResponse<List<StatsTrendValue>> bucketResponse = new BucketResponse<>();
            bucketResponse.setKey(String.valueOf(bucket.getKey()));
            if(nesting.size() == 1) {
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
        List<StatsTrendValue> statsValueList = Lists.newArrayList();

        final String metricKey = Utils.getExtendedStatsAggregationKey(field);
        final String percentileMetricKey = Utils.getPercentileAggregationKey(field);

        for (Histogram.Bucket bucket : buckets) {
            StatsTrendValue statsTrendValue = new StatsTrendValue();
            DateTime key = (DateTime) bucket.getKey();
            statsTrendValue.setPeriod(key.getMillis());

            val statAggregation = bucket.getAggregations()
                    .getAsMap()
                    .get(metricKey);
            statsTrendValue.setStats(Utils.toStats(statAggregation));
            final Aggregation rawPercentiles = bucket.getAggregations()
                    .getAsMap()
                    .get(percentileMetricKey);
            if(null != rawPercentiles) {
                Percentiles internalPercentile = Percentiles.class.cast(rawPercentiles);
                statsTrendValue.setPercentiles(Utils.createPercentilesResponse(internalPercentile));
            }
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

