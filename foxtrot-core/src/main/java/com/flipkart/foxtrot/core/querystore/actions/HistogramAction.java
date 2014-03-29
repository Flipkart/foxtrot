package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 29/03/14
 * Time: 9:22 PM
 */
@AnalyticsProvider(request = HistogramRequest.class, cacheable = true, cacheToken = "histogram")
public class HistogramAction extends Action<HistogramRequest> {
    public HistogramAction(HistogramRequest parameter,
                              DataStore dataStore,
                              ElasticsearchConnection connection,
                              String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        HistogramRequest query = getParameter();
        for(Filter filter : query.getFilters()) {
            filterHashKey += 31 * filter.hashCode();
        }

        return String.format("%s-%d-%d-%d-%s-%s", query.getTable(),
                query.getFrom(), query.getTo(), filterHashKey, query.getPeriod().name(), query.getField());
    }

    @Override
    public ActionResponse execute(HistogramRequest parameter) throws QueryStoreException {
        try {
            /*if(!tableManager.exists(query.getTable())) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE,
                        "There is no table called: " + query.getTable());
            }*/
            DateHistogram.Interval interval = null;
            switch (parameter.getPeriod()) {
                case minutes:
                    interval = DateHistogram.Interval.MINUTE;
                    break;
                case hours:
                    interval = DateHistogram.Interval.HOUR;
                    break;
                case days:
                    interval = DateHistogram.Interval.DAY;
                    break;
            }
            SearchResponse response = getConnection().getClient().prepareSearch(
                                                    ElasticsearchUtils.getIndices(parameter.getTable()))
                    .setTypes(ElasticsearchUtils.TYPE_NAME)
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                            .genFilter(parameter.getFilters())
                            .must(QueryBuilders.rangeQuery(parameter.getField())
                                    .from(parameter.getFrom())
                                    .to(parameter.getTo()))
                    )
                    .setSize(0)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(AggregationBuilders.dateHistogram(parameter.getField())
                            .field(parameter.getField())
                            .interval(interval))
                    .execute()
                    .actionGet();
            DateHistogram dateHistogram = response.getAggregations().get(parameter.getField());
            Collection<? extends DateHistogram.Bucket> buckets = dateHistogram.getBuckets();
            List<HistogramResponse.Count> counts
                                            = new ArrayList<HistogramResponse.Count>(buckets.size());
            for(DateHistogram.Bucket bucket : buckets) {
                HistogramResponse.Count count = new HistogramResponse.Count(
                                                        bucket.getKeyAsNumber(), bucket.getDocCount());
                counts.add(count);
            }
            return new HistogramResponse(counts);
        } catch (Exception e) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.HISTOGRAM_GENERATION_ERROR,
                    "Malformed query", e);
        }
    }
}
