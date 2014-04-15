package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

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
        if(query.getFilters() != null) {
            for(Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        if(query.getValues() != null) {
            for(String value : query.getValues()) {
                filterHashKey += 31 * value.hashCode();
            }
        }

        return String.format("%s-%s-%d-%d-%d", query.getTable(),
                query.getField(), query.getFrom()/30000, query.getTo()/30000, filterHashKey);
    }

    @Override
    public ActionResponse execute(TrendRequest parameter) throws QueryStoreException {
        long currentTime = System.currentTimeMillis();
        if(0L == parameter.getFrom() || 0L == parameter.getTo()) {
            parameter.setFrom(currentTime - 86400000L);
            parameter.setTo(currentTime);
        }
        String field = parameter.getField();
        if(null == field) {
            field = "all";
        }
        if(null != parameter.getValues() && parameter.getField().equalsIgnoreCase("all")) {
            List<Filter> filters = Lists.newArrayList();
            for(String value : parameter.getValues()) {
                filters.add(new EqualsFilter(field, value));
            }
            if( parameter.getFilters() != null) {
                parameter.getFilters().addAll(filters);
            }
            else {
                parameter.setFilters(filters);
            }
        }
        SearchRequestBuilder query = getConnection().getClient()
                .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable()))
                .addAggregation(AggregationBuilders.terms(field).field(field)
                .subAggregation(AggregationBuilders.histogram(field).field("timestamp").interval(86400000L)));
        SearchResponse response = query.execute().actionGet();
        Map<String, List<TrendResponse.Count>> trendCounts = new TreeMap<String, List<TrendResponse.Count>>();
        Terms terms = response.getAggregations().get(field);
        for(Terms.Bucket bucket : terms.getBuckets()) {
            final String key = bucket.getKeyAsText().string();
            List<TrendResponse.Count> counts = Lists.newArrayList();
            Aggregations subAggregations = bucket.getAggregations();
            Histogram histogram = subAggregations.get(field);
            for(Histogram.Bucket histogramBucket : histogram.getBuckets()) {
                counts.add(new TrendResponse.Count(histogramBucket.getKeyAsNumber(), histogramBucket.getDocCount()));
            }
            trendCounts.put(key,counts);
        }
        return new TrendResponse(trendCounts);
    }
    
}
