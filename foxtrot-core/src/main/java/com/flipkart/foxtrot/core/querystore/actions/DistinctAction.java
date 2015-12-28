package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rishabh.goyal on 17/11/14.
 */

@AnalyticsProvider(opcode = "distinct", request = DistinctRequest.class, response = DistinctResponse.class, cacheable = true)
public class DistinctAction extends Action<DistinctRequest> {
    private static final Logger logger = LoggerFactory.getLogger(GroupAction.class.getSimpleName());

    public DistinctAction(DistinctRequest request,
                          TableMetadataManager tableMetadataManager,
                          DataStore dataStore,
                          QueryStore queryStore,
                          ElasticsearchConnection connection,
                          String cacheToken) {
        super(request, tableMetadataManager, dataStore, queryStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        DistinctRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (int i = 0; i < query.getNesting().size(); i++){
            filterHashKey += 31 * query.getNesting().get(i).hashCode() * (i+1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(DistinctRequest request) throws QueryStoreException {
        request.setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
        if (request.getTable() == null) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid table name");
        }
        if (null == request.getFilters()) {
            request.setFilters(Lists.<Filter>newArrayList(new AnyFilter(request.getTable())));
        }
        try {
            SearchRequestBuilder query = getConnection().getClient()
                                               .prepareSearch(ElasticsearchUtils.getIndices(request.getTable(), request))
                                               .setIndicesOptions(Utils.indicesOptions());
            TermsBuilder rootBuilder = null;
            TermsBuilder termsBuilder = null;

            for (ResultSort nestedField : request.getNesting()) {
                if (nestedField.getField() == null || nestedField.getField().trim().isEmpty()) {
                    throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Illegal Nesting Parameters");
                }

                String aggregationKey = Utils.sanitizeFieldForAggregation(nestedField.getField());
                Terms.Order order = (nestedField.getOrder() == ResultSort.Order.desc) ? Terms.Order.term(false) : Terms.Order.term(true);

                if (null == termsBuilder) {
                    termsBuilder = AggregationBuilders.terms(aggregationKey).field(nestedField.getField()).order(order);
                } else {
                    TermsBuilder tempBuilder = AggregationBuilders.terms(aggregationKey).field(nestedField.getField()).order(order);
                    termsBuilder.subAggregation(tempBuilder);
                    termsBuilder = tempBuilder;
                }
                termsBuilder.size(0);
                if (null == rootBuilder) {
                    rootBuilder = termsBuilder;
                }
            }
            query.setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                    .genFilter(request.getFilters()))
                    .setSearchType(SearchType.COUNT)
                    .addAggregation(rootBuilder);
            SearchResponse response = query.execute().actionGet();
            Aggregations aggregations = response.getAggregations();
            // Check if any aggregation is present or not
            if (aggregations == null) {
                logger.error("Null response for Group. Request : " + request.toString());
                return new DistinctResponse(new ArrayList<String>(), new ArrayList<List<String>>());
            }
            return getDistinctResponse(request, aggregations);
        } catch (QueryStoreException ex) {
            throw ex;
        } catch (Exception e) {
            logger.error("Error running grouping: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running group query.", e);
        }
    }

    @Override
    protected void validate() throws QueryStoreException {
        String tableName = ElasticsearchUtils.getValidTableName(getParameter().getTable());
        try {
            if (tableName == null) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Table cannot be null");
            } else if (!getTableMetadataManager().exists(tableName)) {
                throw new QueryStoreException(QueryStoreException.ErrorCode.NO_SUCH_TABLE, "Table not found");
            }
        } catch (QueryStoreException e) {
            logger.error("Table is null or not found.", getParameter().getTable());
            throw e;
        } catch (Exception e) {
            logger.error("Error while checking table's existence.", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR, "Error while fetching metadata");
        }

    }

    private DistinctResponse getDistinctResponse(DistinctRequest request, Aggregations aggregations) {
        DistinctResponse response = new DistinctResponse();
        List<String> headerList = new ArrayList<String>();
        for (ResultSort nestedField : request.getNesting()){
            headerList.add(nestedField.getField());
        }
        response.setHeaders(headerList);

        List<List<String>> responseList = new ArrayList<List<String>>();
        flatten(null, headerList, responseList, aggregations);
        response.setResult(responseList);
        return response;
    }

    private void flatten(String parentKey, List<String> fields, List<List<String>> responseList, Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size())
                : new ArrayList<String>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                responseList.add(getValueList(parentKey, bucket.getKey()));
            } else {
                flatten(getProperKey(parentKey, bucket.getKey()), remainingFields, responseList, bucket.getAggregations());
            }
        }
    }

    private String getProperKey(String parentKey, String currentKey) {
        return parentKey == null ? currentKey : parentKey + Constants.SEPARATOR + currentKey;
    }

    private List<String> getValueList(String parentKey, String currentKey){
        String finalValue = getProperKey(parentKey, currentKey);
        String[] valuesList = finalValue.split(Constants.SEPARATOR);
        return Arrays.asList(valuesList);
    }
}
