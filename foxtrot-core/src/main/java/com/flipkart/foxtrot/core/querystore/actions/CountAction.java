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
        filterHashKey += 31 * (request.getColumn() != null ? request.getColumn().hashCode() : "COLUMN".hashCode());
        return String.format("count-%s-%d", request.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(CountRequest parameter) throws QueryStoreException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters() || parameter.getFilters().isEmpty()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }

        if (parameter.getColumn() != null) {
            parameter.getFilters().add(new ExistsFilter(parameter.getColumn()));
        }
        CountRequestBuilder countRequestBuilder = null;
        org.elasticsearch.action.count.CountResponse countResponse;
        try {
            countRequestBuilder = getConnection().getClient()
                    .prepareCount(ElasticsearchUtils.getIndices(parameter.getTable()))
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()));
            countResponse = countRequestBuilder.execute().actionGet();
            return new CountResponse(countResponse.getCount());
        } catch (Exception e) {
            if (null != countRequestBuilder) {
                logger.error("Error running count query: " + countRequestBuilder, e);
            } else {
                logger.error("Count query generation error: ", e);
            }
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running query: " + parameter.toString());
        }
    }
}
