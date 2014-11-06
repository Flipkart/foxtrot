package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 02/11/14.
 */

@AnalyticsProvider(opcode = "count", request = CountRequest.class, response = CountResponse.class, cacheable = false)
public class CountAction extends Action<CountRequest> {

    private static final Logger logger = LoggerFactory.getLogger(CountAction.class);

    public CountAction(CountRequest parameter, DataStore dataStore, ElasticsearchConnection connection, String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        CountRequest request = getParameter();
        if (null != request.getFilters()) {
            for (Filter filter : request.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }

        filterHashKey += 31 * (request.isDistinct() ? "TRUE".hashCode() : "FALSE".hashCode() );
        filterHashKey += 31 * (request.getField() != null ? request.getField().hashCode() : "COLUMN".hashCode());
        return String.format("count-%s-%d", request.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(CountRequest parameter) throws QueryStoreException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters() || parameter.getFilters().isEmpty()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }

        // Null field implies complete doc count
        if (parameter.getField() != null) {
            parameter.getFilters().add(new ExistsFilter(parameter.getField()));
        }

        try {
            if (parameter.isDistinct()){
                SearchRequestBuilder query = getConnection().getClient()
                        .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable()))
                        .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                                .genFilter(parameter.getFilters()))
                        .addAggregation(AggregationBuilders
                                .terms(Utils.sanitizeFieldForAggregation(parameter.getField()))
                                .field(parameter.getField())
                                .size(0));
                SearchResponse response = query.execute().actionGet();
                Aggregations aggregations = response.getAggregations();
                Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(parameter.getField()));
                if (terms == null){
                    return new CountResponse(0);
                } else {
                    return new CountResponse(terms.getBuckets().size());
                }
            } else {
                CountRequestBuilder countRequestBuilder = getConnection().getClient()
                        .prepareCount(ElasticsearchUtils.getIndices(parameter.getTable()))
                        .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()));
                org.elasticsearch.action.count.CountResponse countResponse = countRequestBuilder.execute().actionGet();
                return new CountResponse(countResponse.getCount());
            }
        } catch (Exception e) {
            logger.error("Error running count: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running count query.", e);
        }
    }
}
