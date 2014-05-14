package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 10:27 PM
 */
@AnalyticsProvider(opcode = "trend", request = TrendRequest.class, response = TrendResponse.class, cacheable = true)
public class TrendAction extends Action<TrendRequest> {
    private static final Logger logger = LoggerFactory.getLogger(TrendAction.class.getSimpleName());

    public TrendAction(TrendRequest parameter,
                       DataStore dataStore,
                       ElasticsearchConnection connection,
                       String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        TrendRequest query = getParameter();
        long filterHashKey = 0L;
        if (query.getFilters() != null) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        if (query.getValues() != null) {
            for (String value : query.getValues()) {
                filterHashKey += 31 * value.hashCode();
            }
        }

        filterHashKey += 31 * query.getInterval();
        filterHashKey += 31 * query.getTimestamp().hashCode();
        filterHashKey += 31 * (query.getField() != null ? query.getField().hashCode() : "FIELD".hashCode());

        return String.format("%s-%s-%d-%d-%d", query.getTable(),
                query.getField(), query.getFrom() / 30000, query.getTo() / 30000, filterHashKey);
    }

    @Override
    public ActionResponse execute(TrendRequest parameter) throws QueryStoreException {
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        long currentTime = System.currentTimeMillis();
        if (0L == parameter.getFrom() || 0L == parameter.getTo()) {
            parameter.setFrom(currentTime - 86400000L);
            parameter.setTo(currentTime);
        }
        String field = parameter.getField();
        if (null == field) {
            field = "all";
        }
        if (null != parameter.getValues() && !parameter.getField().equalsIgnoreCase("all")) {
            List<Filter> filters = Lists.newArrayList();
            for (String value : parameter.getValues()) {
                filters.add(new EqualsFilter(field, value));
            }
            parameter.getFilters().addAll(filters);
        }

        try {
            SearchRequestBuilder query = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable()))
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()))
                    .addAggregation(AggregationBuilders.terms(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                            ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE)).field(field)
                            .subAggregation(AggregationBuilders.histogram(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                                    ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE))
                                    .field(parameter.getTimestamp()).interval(parameter.getInterval())));
            SearchResponse response = query.execute().actionGet();
            Map<String, List<TrendResponse.Count>> trendCounts = new TreeMap<String, List<TrendResponse.Count>>();
            Terms terms = response.getAggregations().get(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                    ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE));
            for (Terms.Bucket bucket : terms.getBuckets()) {
                final String key = bucket.getKeyAsText().string();
                List<TrendResponse.Count> counts = Lists.newArrayList();
                Aggregations subAggregations = bucket.getAggregations();
                Histogram histogram = subAggregations.get(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                        ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE));
                for (Histogram.Bucket histogramBucket : histogram.getBuckets()) {
                    counts.add(new TrendResponse.Count(histogramBucket.getKeyAsNumber(), histogramBucket.getDocCount()));
                }
                trendCounts.put(key, counts);
            }
            return new TrendResponse(trendCounts);
        } catch (Exception e) {
            logger.error("Error running trend action: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running trend action.", e);
        }
    }

}
