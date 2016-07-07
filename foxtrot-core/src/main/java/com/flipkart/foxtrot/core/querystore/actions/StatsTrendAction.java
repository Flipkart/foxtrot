package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
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
import com.google.common.collect.Maps;
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
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
        DateHistogramInterval interval;
        switch (request.getPeriod()) {
            case seconds:
                interval = DateHistogramInterval.SECOND;
                break;
            case minutes:
                interval = DateHistogramInterval.MINUTE;
                break;
            case hours:
                interval = DateHistogramInterval.HOUR;
                break;
            case days:
                interval = DateHistogramInterval.DAY;
                break;
            default:
                interval = DateHistogramInterval.HOUR;
                break;
        }

        String dateHistogramKey = Utils.getDateHistogramKey(request.getTimestamp());
        return AggregationBuilders.dateHistogram(dateHistogramKey)
                .field(request.getTimestamp())
                .interval(interval)
                .subAggregation(Utils.buildExtendedStatsAggregation(request.getField()))
                .subAggregation(Utils.buildPercentileAggregation(request.getField()));
    }

    private StatsTrendResponse buildResponse(StatsTrendRequest request, Aggregations aggregations) {
        String dateHistogramKey = Utils.getDateHistogramKey(request.getTimestamp());
        Histogram dateHistogram = aggregations.get(dateHistogramKey);
        Collection<? extends Histogram.Bucket> buckets = dateHistogram.getBuckets();

        String metricKey = Utils.getExtendedStatsAggregationKey(request.getField());
        String percentileMetricKey = Utils.getPercentileAggregationKey(request.getField());

        List<StatsTrendValue> statsValueList = new ArrayList<StatsTrendValue>();
        for (Histogram.Bucket bucket : buckets) {
            StatsTrendValue statsTrendValue = new StatsTrendValue();
            DateTime key = (DateTime) bucket.getKey();
            statsTrendValue.setPeriod(key.getMillis());

            ExtendedStats extendedStats = bucket.getAggregations().get(metricKey);
            Map<String, Number> stats = Maps.newHashMap();
            stats.put("avg", extendedStats.getAvg());
            stats.put("sum", extendedStats.getSum());
            stats.put("count", extendedStats.getCount());
            stats.put("min", extendedStats.getMin());
            stats.put("max", extendedStats.getMax());
            stats.put("sum_of_squares", extendedStats.getSumOfSquares());
            stats.put("variance", extendedStats.getVariance());
            stats.put("std_deviation", extendedStats.getStdDeviation());
            statsTrendValue.setStats(stats);

            Percentiles percentilesResponse = bucket.getAggregations().get(percentileMetricKey);
            Map<Number, Number> percentiles = Maps.newHashMap();
            for (Percentile percentile : percentilesResponse) {
                percentiles.put(percentile.getPercent(), percentile.getValue());
            }
            statsTrendValue.setPercentiles(percentiles);

            statsValueList.add(statsTrendValue);
        }
        return new StatsTrendResponse(statsValueList);
    }

    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

}

