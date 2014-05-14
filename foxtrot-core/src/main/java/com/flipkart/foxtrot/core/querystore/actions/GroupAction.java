package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 7:16 PM
 */
@AnalyticsProvider(opcode = "group", request = GroupRequest.class, response = GroupResponse.class, cacheable = true)
public class GroupAction extends Action<GroupRequest> {
    private static final Logger logger = LoggerFactory.getLogger(GroupAction.class.getSimpleName());

    public GroupAction(GroupRequest parameter,
                       DataStore dataStore,
                       ElasticsearchConnection connection,
                       String cacheToken) {
        super(parameter, dataStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        GroupRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (String field : query.getNesting()) {
            filterHashKey += 31 * field.hashCode();
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(GroupRequest parameter) throws QueryStoreException {
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        try {
            SearchRequestBuilder query = getConnection().getClient().prepareSearch(ElasticsearchUtils.getIndices(
                    parameter.getTable()));
            TermsBuilder rootBuilder = null;
            TermsBuilder termsBuilder = null;
            for (String field : parameter.getNesting()) {
                if ( field == null || field.trim().isEmpty() ){
                    throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Illegal Nesting Parameters");
                }
                if (null == termsBuilder) {
                    termsBuilder = AggregationBuilders.terms(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                            ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE)).field(field);
                } else {
                    TermsBuilder tempBuilder = AggregationBuilders.terms(
                            field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                                    ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE)).field(field);
                    termsBuilder.subAggregation(tempBuilder);
                    termsBuilder = tempBuilder;
                }
                if (null == rootBuilder) {
                    rootBuilder = termsBuilder;
                }
            }
            query.setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                    .genFilter(parameter.getFilters()))
                    .addAggregation(rootBuilder);
            SearchResponse response = query.execute().actionGet();
            List<String> fields = parameter.getNesting();
            Aggregations aggregations = response.getAggregations();
            return new GroupResponse(getMap(fields, aggregations));
        } catch (QueryStoreException ex){
          throw ex;
        } catch (Exception e) {
            logger.error("Error running grouping: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running group query.", e);
        }
    }

    private Map<String, Object> getMap(List<String> fields, Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size())
                : new ArrayList<String>();
        Terms terms = aggregations.get(field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX,
                ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE));
        Map<String, Object> levelCount = new HashMap<String, Object>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                levelCount.put(bucket.getKey(), bucket.getDocCount());
            } else {
                levelCount.put(bucket.getKey(), getMap(remainingFields, bucket.getAggregations()));
            }
        }
        return levelCount;

    }

}
