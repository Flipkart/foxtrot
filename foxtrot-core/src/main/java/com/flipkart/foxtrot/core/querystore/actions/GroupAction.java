/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 7:16 PM
 */
@AnalyticsProvider(opcode = "group", request = GroupRequest.class, response = GroupResponse.class, cacheable = true)
public class GroupAction extends Action<GroupRequest> {

    public GroupAction(GroupRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken,
                       CacheManager cacheManager) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager);
    }

    @Override
    protected void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
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

        if (null != query.getUniqueCountOn()) {
            filterHashKey += 31 * query.getUniqueCountOn().hashCode();
        }

        for (int i = 0; i < query.getNesting().size(); i++) {
            filterHashKey += 31 * query.getNesting().get(i).hashCode() * (i + 1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public void validateImpl(GroupRequest parameter) throws MalformedQueryException {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (CollectionUtils.isNullOrEmpty(parameter.getNesting())) {
            validationErrors.add("at least one grouping parameter is required");
        } else {
            validationErrors.addAll(parameter.getNesting().stream()
                    .filter(CollectionUtils::isNullOrEmpty)
                    .map(field -> "grouping parameter cannot have null or empty name")
                    .collect(Collectors.toList()));
        }

        if (parameter.getUniqueCountOn() != null && parameter.getUniqueCountOn().isEmpty()) {
            validationErrors.add("unique field cannot be empty (can be null)");
        }

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }


    @Override
    public ActionResponse execute(GroupRequest parameter) throws FoxtrotException {
        SearchRequestBuilder query;
        try {
            query = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setIndicesOptions(Utils.indicesOptions());
            AbstractAggregationBuilder aggregation = buildAggregation();
            query.setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                    .genFilter(parameter.getFilters()))
                    .setSize(0)
                    .addAggregation(aggregation);
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        try {
            SearchResponse response = query.execute().actionGet();
            List<String> fields = parameter.getNesting();
            Aggregations aggregations = response.getAggregations();
            // Check if any aggregation is present or not
            if (aggregations == null) {
                return new GroupResponse(Collections.<String, Object>emptyMap());
            }
            return new GroupResponse(getMap(fields, aggregations));
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    private AbstractAggregationBuilder buildAggregation() {
        TermsBuilder rootBuilder = null;
        TermsBuilder termsBuilder = null;
        for (String field : getParameter().getNesting()) {
            if (null == termsBuilder) {
                termsBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field)).field(field);
            } else {
                TermsBuilder tempBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field)).field(field);
                termsBuilder.subAggregation(tempBuilder);
                termsBuilder = tempBuilder;
            }
            termsBuilder.size(0);
            if (null == rootBuilder) {
                rootBuilder = termsBuilder;
            }
        }

        if (!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
            assert termsBuilder != null;
            termsBuilder.subAggregation(Utils.buildCardinalityAggregation(getParameter().getUniqueCountOn()));
        }

        return rootBuilder;
    }

    private Map<String, Object> getMap(List<String> fields, Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size())
                : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        Map<String, Object> levelCount = Maps.newHashMap();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                if (!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
                    String key = Utils.sanitizeFieldForAggregation(getParameter().getUniqueCountOn());
                    Cardinality cardinality = bucket.getAggregations().get(key);
                    levelCount.put(String.valueOf(bucket.getKey()), cardinality.getValue());
                } else {
                    levelCount.put(String.valueOf(bucket.getKey()), bucket.getDocCount());
                }
            } else {
                levelCount.put(String.valueOf(bucket.getKey()), getMap(remainingFields, bucket.getAggregations()));
            }
        }
        return levelCount;

    }

}
