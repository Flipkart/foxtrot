package com.flipkart.foxtrot.core.querystore.actions;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 17/11/14.
 */

@AnalyticsProvider(opcode = "distinct", request = DistinctRequest.class, response = DistinctResponse.class, cacheable
        = true)
public class DistinctAction extends Action<DistinctRequest> {

    private static final Logger logger = LoggerFactory.getLogger(DistinctAction.class);

    public DistinctAction(DistinctRequest parameter, String cacheToken, AnalyticsLoader analyticsLoader) {
        super(parameter, cacheToken, analyticsLoader);
    }

    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public void validateImpl(DistinctRequest parameter, String email) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (CollectionUtils.isNullOrEmpty(parameter.getNesting())) {
            validationErrors.add("At least one nesting parameter is required");
        } else {
            for (ResultSort resultSort : com.collections.CollectionUtils.nullSafeList(parameter.getNesting())) {

                if (CollectionUtils.isNullOrEmpty(resultSort.getField())) {
                    validationErrors.add("nested parameter cannot have null name");
                }
                if (resultSort.getOrder() == null) {
                    validationErrors.add("nested parameter cannot have null sorting order");
                }

            }
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public String getRequestCacheKey() {
        long filterHashKey = 0L;
        DistinctRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (int i = 0; i < query.getNesting()
                .size(); i++) {
            filterHashKey += 31 * query.getNesting()
                    .get(i)
                    .hashCode() * (i + 1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(DistinctRequest request) {
        SearchRequestBuilder query;
        try {
            query = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(request.getTable(), request))
                    .setIndicesOptions(Utils.indicesOptions());
            query.setQuery(new ElasticSearchQueryGenerator().genFilter(request.getFilters()))
                    .setSize(QUERY_SIZE)
                    .addAggregation(Utils.buildTermsAggregation(request.getNesting(), Sets.newHashSet()));
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(request, e);
        }

        try {
            SearchResponse response = query.execute()
                    .actionGet(getGetQueryTimeout());
            return getResponse(response, getParameter());
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(request, e);
        }
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public SearchRequestBuilder getRequestBuilder(DistinctRequest request) {
        SearchRequestBuilder query;
        try {
            query = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(request.getTable(), request))
                    .setIndicesOptions(Utils.indicesOptions());
            query.setQuery(new ElasticSearchQueryGenerator().genFilter(request.getFilters()))
                    .setSize(QUERY_SIZE)
                    .addAggregation(Utils.buildTermsAggregation(request.getNesting(), Sets.newHashSet()));

        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(request, e);
        }
        return query;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, DistinctRequest parameter) {
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        // Check if any aggregation is present or not
        if (aggregations == null) {
            logger.error("Null response for Group. Request : {}", parameter);
            return new DistinctResponse(new ArrayList<>(), new ArrayList<>());
        }
        return getDistinctResponse(parameter, aggregations);
    }

    private DistinctResponse getDistinctResponse(DistinctRequest request, Aggregations aggregations) {
        DistinctResponse response = new DistinctResponse();
        List<String> headerList = request.getNesting()
                .stream()
                .map(ResultSort::getField)
                .collect(Collectors.toList());
        response.setHeaders(headerList);

        List<List<String>> responseList = new ArrayList<>();
        flatten(null, headerList, responseList, aggregations);
        response.setResult(responseList);
        return response;
    }

    private void flatten(String parentKey, List<String> fields, List<List<String>> responseList,
            Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size()) : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                responseList.add(getValueList(parentKey, String.valueOf(bucket.getKey())));
            } else {
                flatten(getProperKey(parentKey, String.valueOf(bucket.getKey())), remainingFields, responseList,
                        bucket.getAggregations());
            }
        }
    }

    private String getProperKey(String parentKey, String currentKey) {
        return parentKey == null ? currentKey : parentKey + Constants.SEPARATOR + currentKey;
    }

    private List<String> getValueList(String parentKey, String currentKey) {
        String finalValue = getProperKey(parentKey, currentKey);
        String[] valuesList = finalValue.split(Constants.SEPARATOR);
        return Arrays.asList(valuesList);
    }
}
