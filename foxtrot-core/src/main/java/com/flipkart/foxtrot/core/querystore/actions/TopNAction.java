package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.top.TopNParams;
import com.flipkart.foxtrot.common.top.TopNRequest;
import com.flipkart.foxtrot.common.top.TopNResponse;
import com.flipkart.foxtrot.common.top.ValueCount;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by rishabh.goyal on 28/02/15.
 */


@AnalyticsProvider(opcode = "topn", request = TopNRequest.class, response = TopNResponse.class, cacheable = true)
public class TopNAction extends Action<TopNRequest> {

    private static final Logger logger = LoggerFactory.getLogger(TopNAction.class.getSimpleName());

    public TopNAction(TopNRequest parameter,
                      TableMetadataManager tableMetadataManager,
                      DataStore dataStore,
                      QueryStore queryStore,
                      ElasticsearchConnection connection, String cacheToken) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        TopNRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (int i = 0; i < query.getParams().size(); i++) {
            filterHashKey += 31 * query.getParams().get(i).hashCode() * (i + 1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(TopNRequest parameter) throws QueryStoreException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        if (parameter.getTable() == null) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid Table");
        }
        try {
            SearchRequestBuilder query = getConnection().getClient().prepareSearch(ElasticsearchUtils.getIndices(
                    parameter.getTable()));

            List<TermsBuilder> termsBuilders = new ArrayList<TermsBuilder>();
            for (TopNParams param : parameter.getParams()) {
                if (param.getField() == null || param.getField().trim().isEmpty() || param.getCount() < 0) {
                    throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Illegal field-value/count-value");
                }
                if (param.isApproxCount()) {
                    termsBuilders.add(AggregationBuilders
                            .terms(Utils.sanitizeFieldForAggregation(param.getField()))
                            .field(param.getField())
                            .size(param.getCount())
                            .order(Terms.Order.count(false)));
                } else {
                    termsBuilders.add(AggregationBuilders
                            .terms(Utils.sanitizeFieldForAggregation(param.getField()))
                            .field(param.getField())
                            .size(param.getCount())
                                    // This ensures all values are passed between coordinating nodes, however only required number of values are passed to client
                            .shardSize(Integer.MAX_VALUE)
                            .order(Terms.Order.count(false)));
                }
            }
            query.setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                    .genFilter(parameter.getFilters()));

            for (TermsBuilder termsBuilder : termsBuilders) {
                query.addAggregation(termsBuilder);
            }
            SearchResponse response = query.execute().actionGet();

            Aggregations aggregations = response.getAggregations();
            // Check if any aggregation is present or not
            if (aggregations == null) {
                logger.error("NULL_RESPONSE query=" + parameter.toString());
                return new TopNResponse(Collections.<String, List<ValueCount>>emptyMap());
            }
            return convertAggregationsToTopNResponse(aggregations);
        } catch (QueryStoreException ex) {
            throw ex;
        } catch (Exception e) {
            logger.error("ERROR_RUNNING_TOPN_QUERY query=" + getParameter(), e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "ERROR_RUNNING_TOPN_QUERY query=" + getParameter(), e);
        }
    }

    private TopNResponse convertAggregationsToTopNResponse(Aggregations aggregations) {
        TopNResponse topNResponse = new TopNResponse();
        for (TopNParams topNParams : getParameter().getParams()) {
            String field = topNParams.getField();
            String aggregationKey = Utils.sanitizeFieldForAggregation(field);
            Terms terms = aggregations.get(aggregationKey);

            List<ValueCount> counts = new ArrayList<ValueCount>();
            if (terms != null) {
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    counts.add(new ValueCount(bucket.getKey(), bucket.getDocCount()));
                }
            }
            topNResponse.addFieldData(field, counts);
        }
        return topNResponse;
    }
}
