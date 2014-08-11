package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
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
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = "stats", request = StatsRequest.class, response = StatsResponse.class, cacheable = false)
public class StatsAction extends Action<StatsRequest> {

    private static final Logger logger = LoggerFactory.getLogger(StatsAction.class.getSimpleName());

    public StatsAction(StatsRequest parameter, DataStore dataStore, ElasticsearchConnection connection, String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
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
        statsHashKey += 31 * statsRequest.getMetric().hashCode();
        statsHashKey += 31 * statsRequest.getCombiner().hashCode();
        return String.format("%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public ActionResponse execute(StatsRequest parameter) throws QueryStoreException {
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

    private AbstractAggregationBuilder buildAggregation(StatsRequest statsRequest) {
        String metricKey = statsRequest.getField()
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);
        switch (statsRequest.getMetric()) {
            case avg:
                return AggregationBuilders.avg(metricKey).field(statsRequest.getField());
            case sum:
                return AggregationBuilders.sum(metricKey).field(statsRequest.getField());
            case count:
                return AggregationBuilders.count(metricKey).field(statsRequest.getField());
            case min:
                return AggregationBuilders.min(metricKey).field(statsRequest.getField());
            case max:
                return AggregationBuilders.max(metricKey).field(statsRequest.getField());
            default:
                return AggregationBuilders.stats(metricKey).field(statsRequest.getField());
        }
    }

    private StatsResponse buildResponse(StatsRequest request, Aggregations aggregations) {
        String metricKey = request.getField()
                .replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);
        Aggregation aggregation = aggregations.getAsMap().get(metricKey);

        Map<String, Object> result = new HashMap<String, Object>();
        switch (request.getMetric()) {
            case avg:
                InternalAvg internalAvg = (InternalAvg) aggregation;
                result.put("avg", internalAvg.getValue());
                break;
            case sum:
                InternalSum internalSum = (InternalSum) aggregation;
                result.put("sum", internalSum.getValue());
                break;
            case count:
                InternalValueCount internalValueCount = (InternalValueCount) aggregation;
                result.put("count", internalValueCount.getValue());
                break;
            case min:
                InternalMin internalMin = (InternalMin) aggregation;
                result.put("min", internalMin.getValue());
                break;
            case max:
                InternalMax internalMax = (InternalMax) aggregation;
                result.put("max", internalMax.getValue());
                break;
            default:
                InternalStats internalStats = (InternalStats) aggregation;
                result.put("avg", internalStats.getAvg());
                result.put("sum", internalStats.getSum());
                result.put("count", internalStats.getCount());
                result.put("min", internalStats.getMin());
                result.put("max", internalStats.getMax());
                break;
        }
        return new StatsResponse(result);
    }
}

