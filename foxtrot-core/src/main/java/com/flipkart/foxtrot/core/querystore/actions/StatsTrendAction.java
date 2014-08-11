package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "statstrend", request = StatsTrendRequest.class, response = StatsTrendResponse.class, cacheable = false)
public class StatsTrendAction extends Action<StatsTrendRequest> {

    private static final Logger logger = LoggerFactory.getLogger(StatsAction.class.getSimpleName());

    public StatsTrendAction(StatsTrendRequest parameter, DataStore dataStore, ElasticsearchConnection connection, String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long statsHashKey = 0L;
        StatsTrendRequest statsRequest = getParameter();
        if (null != statsRequest.getFilters()) {
            for (Filter filter : statsRequest.getFilters()) {
                statsHashKey += 31 * filter.hashCode();
            }
        }

        statsHashKey += 31 * statsRequest.getPeriod().hashCode();
        statsHashKey += 31 * statsRequest.getMetric().hashCode();
        statsHashKey += 31 * statsRequest.getCombiner().hashCode();
        return String.format("%s-trend-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public ActionResponse execute(StatsTrendRequest parameter) throws QueryStoreException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        if (null == parameter.getTable()) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid Table");
        }

        try {
            AbstractAggregationBuilder aggregation = buildAggregation(parameter);
            SearchResponse response = getConnection().getClient().prepareSearch(
                    ElasticsearchUtils.getIndices(parameter.getTable()))
                    .setTypes(ElasticsearchUtils.TYPE_NAME)
                    .setSize(0)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(aggregation)
                    .execute()
                    .actionGet();

            Aggregations aggregations = response.getAggregations();
            if (aggregations != null) {
                return buildResponse(parameter, aggregations);
            }
            return null;
        } catch (Exception e) {
            logger.error("Error running stats query: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running stats query.", e);
        }
    }

    private AbstractAggregationBuilder buildSingleStatsAggregation(StatsTrendRequest request) {
        String metricKey = request.getField()
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);
        switch (request.getMetric()) {
            case avg:
                return AggregationBuilders.avg(metricKey).field(request.getField());
            case sum:
                return AggregationBuilders.sum(metricKey).field(request.getField());
            case count:
                return AggregationBuilders.count(metricKey).field(request.getField());
            case min:
                return AggregationBuilders.min(metricKey).field(request.getField());
            case max:
                return AggregationBuilders.max(metricKey).field(request.getField());
            default:
                return AggregationBuilders.stats(metricKey).field(request.getField());
        }
    }

    private AbstractAggregationBuilder buildAggregation(StatsTrendRequest request) {
        String dateHistogramKey = ActionConstants.TIMESTAMP_FIELD
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);

        DateHistogram.Interval interval = null;
        switch (request.getPeriod()) {
            case minutes:
                interval = DateHistogram.Interval.MINUTE;
                break;
            case hours:
                interval = DateHistogram.Interval.HOUR;
                break;
            case days:
                interval = DateHistogram.Interval.DAY;
                break;
            default:
                interval = DateHistogram.Interval.HOUR;
                break;
        }

        return AggregationBuilders.dateHistogram(dateHistogramKey)
                .field(ActionConstants.TIMESTAMP_FIELD)
                .interval(interval)
                .subAggregation(buildSingleStatsAggregation(request));
    }

    private StatsTrendResponse buildResponse(StatsTrendRequest request, Aggregations aggregations) {
        String dateHistogramKey = ActionConstants.TIMESTAMP_FIELD
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);
        DateHistogram dateHistogram = aggregations.get(dateHistogramKey);
        Collection<? extends DateHistogram.Bucket> buckets = dateHistogram.getBuckets();

        String metricKey = request.getField()
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);

        List<StatsTrendResponse.BucketStats> statsList = new ArrayList<StatsTrendResponse.BucketStats>();
        for (DateHistogram.Bucket bucket : buckets) {
            StatsTrendResponse.BucketStats bucketStats = new StatsTrendResponse.BucketStats();
            bucketStats.setPeriod(bucket.getKeyAsNumber());

            Aggregation internalAggregation = bucket.getAggregations().getAsMap().get(metricKey);
            Map<String, Object> stats = new HashMap<String, Object>();
            switch (request.getMetric()) {
                case avg:
                    InternalAvg internalAvg = (InternalAvg) internalAggregation;
                    stats.put("avg", internalAvg.getValue());
                    break;
                case sum:
                    InternalSum internalSum = (InternalSum) internalAggregation;
                    stats.put("sum", internalSum.getValue());
                    break;
                case count:
                    InternalValueCount internalValueCount = (InternalValueCount) internalAggregation;
                    stats.put("count", internalValueCount.getValue());
                    break;
                case min:
                    InternalMin internalMin = (InternalMin) internalAggregation;
                    stats.put("min", internalMin.getValue());
                    break;
                case max:
                    InternalMax internalMax = (InternalMax) internalAggregation;
                    stats.put("max", internalMax.getValue());
                    break;
                default:
                    InternalStats internalStats = (InternalStats) internalAggregation;
                    stats.put("avg", internalStats.getAvg());
                    stats.put("sum", internalStats.getSum());
                    stats.put("count", internalStats.getCount());
                    stats.put("min", internalStats.getMin());
                    stats.put("max", internalStats.getMax());
                    break;
            }
            bucketStats.setStats(stats);
            statsList.add(bucketStats);
        }
        return new StatsTrendResponse(statsList);
    }
}

