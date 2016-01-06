package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

/**
 * Created by rishabh.goyal on 02/11/14.
 */

@AnalyticsProvider(opcode = "count", request = CountRequest.class, response = CountResponse.class, cacheable = false)
public class CountAction extends Action<CountRequest> {

    public CountAction(CountRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken,
                       CacheManager cacheManager) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager);

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

        filterHashKey += 31 * (request.isDistinct() ? "TRUE".hashCode() : "FALSE".hashCode());
        filterHashKey += 31 * (request.getField() != null ? request.getField().hashCode() : "COLUMN".hashCode());
        return String.format("count-%s-%d", request.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(CountRequest parameter) throws FoxtrotException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters() || parameter.getFilters().isEmpty()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }

        // Null field implies complete doc count
        if (parameter.getField() != null) {
            parameter.getFilters().add(new ExistsFilter(parameter.getField()));
        }

        if (parameter.isDistinct()) {
            SearchRequestBuilder query;
            try {
                query = getConnection().getClient()
                        .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                        .setIndicesOptions(Utils.indicesOptions())
                        .setSearchType(SearchType.COUNT)
                        .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                                .genFilter(parameter.getFilters()))
                        .addAggregation(AggregationBuilders
                                        .cardinality(Utils.sanitizeFieldForAggregation(parameter.getField()))
                                        .field(parameter.getField())
                        );
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(parameter, e);
            }

            try {
                SearchResponse response = query.execute().actionGet();
                Aggregations aggregations = response.getAggregations();
                Cardinality cardinality = aggregations.get(Utils.sanitizeFieldForAggregation(parameter.getField()));
                if (cardinality == null) {
                    return new CountResponse(0);
                } else {
                    return new CountResponse(cardinality.getValue());
                }
            } catch (ElasticsearchException e) {
                throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
            }
        } else {
            CountRequestBuilder countRequestBuilder;
            try {
                countRequestBuilder = getConnection().getClient()
                        .prepareCount(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                        .setIndicesOptions(Utils.indicesOptions())
                        .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()));
            } catch (Exception e) {
                throw FoxtrotExceptions.queryCreationException(parameter, e);
            }
            try {
                org.elasticsearch.action.count.CountResponse countResponse = countRequestBuilder.execute().actionGet();
                return new CountResponse(countResponse.getCount());
            } catch (ElasticsearchException e) {
                throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
            }
        }
    }
}
