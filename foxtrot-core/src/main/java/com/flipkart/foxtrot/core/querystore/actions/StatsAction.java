package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsOperation;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishabh.goyal on 02/08/14.
 */

@AnalyticsProvider(opcode = AnalyticsOperation.stats, request = StatsRequest.class, response = StatsResponse.class, cacheable = false)
public class StatsAction extends Action<StatsRequest> {

    private static final Logger logger = LoggerFactory.getLogger(StatsAction.class.getSimpleName());

    public StatsAction(StatsRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken);
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
        statsHashKey += 31 * statsRequest.getCombiner().hashCode();
        return String.format("%s-%s-%d", statsRequest.getTable(), statsRequest.getField(), statsHashKey);
    }

    @Override
    public ActionResponse execute(StatsRequest request) throws QueryStoreException {
        request.setTable(ElasticsearchUtils.getValidTableName(request.getTable()));
        if (null == request.getFilters()) {
            request.setFilters(Lists.<Filter>newArrayList(new AnyFilter(request.getTable())));
        }
        if (null == request.getTable()) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid Table");
        }

        try {
            SearchResponse response = getConnection().getClient().prepareSearch(
                    ElasticsearchUtils.getIndices(request.getTable(), request))
                    .setTypes(ElasticsearchUtils.TYPE_NAME)
                    .setQuery(new ElasticSearchQueryGenerator(request.getCombiner()).genFilter(request.getFilters()))
                    .setSize(0)
                    .setSearchType(SearchType.COUNT)
                    .addAggregation(Utils.buildExtendedStatsAggregation(request.getField()))
                    .addAggregation(Utils.buildPercentileAggregation(request.getField()))
                    .execute()
                    .actionGet();

            Aggregations aggregations = response.getAggregations();
            if (aggregations != null) {
                return buildResponse(request, aggregations);
            }
            return null;
        } catch (Exception e) {
            logger.error("Error running stats query: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running stats query.", e);
        }
    }

    private StatsResponse buildResponse(StatsRequest request, Aggregations aggregations) {
        String metricKey = Utils.getExtendedStatsAggregationKey(request.getField());
        String percentileMetricKey = Utils.getPercentileAggregationKey(request.getField());

        StatsValue statsValue = new StatsValue();

        InternalExtendedStats extendedStats = InternalExtendedStats.class.cast(aggregations.getAsMap().get(metricKey));
        Map<String, Number> stats = new HashMap<String, Number>();
        stats.put("avg", extendedStats.getAvg());
        stats.put("sum", extendedStats.getSum());
        stats.put("count", extendedStats.getCount());
        stats.put("min", extendedStats.getMin());
        stats.put("max", extendedStats.getMax());
        stats.put("sum_of_squares", extendedStats.getSumOfSquares());
        stats.put("variance", extendedStats.getVariance());
        stats.put("std_deviation", extendedStats.getStdDeviation());
        statsValue.setStats(stats);

        InternalPercentiles internalPercentile = InternalPercentiles.class.cast(aggregations.getAsMap().get(percentileMetricKey));
        Map<Number, Number> percentiles = new HashMap<Number, Number>();
        for (Percentiles.Percentile percentile : internalPercentile) {
            percentiles.put(percentile.getPercent(), percentile.getValue());
        }
        statsValue.setPercentiles(percentiles);
        return new StatsResponse(statsValue);
    }
}

