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
import com.flipkart.foxtrot.common.group.MetricsAggregation;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
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
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.*;

import static com.flipkart.foxtrot.common.group.MetricsAggregation.MetricsAggragationType;

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
    protected String getRequestCacheKey() {
        long filterHashKey = 0L;
        GroupRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        for (int i = 0; i < query.getNesting().size(); i++) {
            filterHashKey += 31 * query.getNesting().get(i).hashCode() * (i + 1);
        }
        return String.format("%s-%d", query.getTable(), filterHashKey);
    }

    @Override
    public ActionResponse execute(GroupRequest parameter) throws FoxtrotException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }

        List<String> errorMessages = new ArrayList<>();
        if (parameter.getTable() == null || parameter.getTable().isEmpty()) {
            errorMessages.add("table name cannot be null/empty");
        }

        for (String field : parameter.getNesting()) {
            if (field == null || field.trim().isEmpty()) {
                errorMessages.add("nesting parameter cannot be null/empty");
                break;
            }
        }

        if (!errorMessages.isEmpty()) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, errorMessages);
        }

        SearchRequestBuilder query;
        try {
            query = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setIndicesOptions(Utils.indicesOptions());
            TermsBuilder rootBuilder = null;
            TermsBuilder termsBuilder = null;
            for (String field : parameter.getNesting()) {
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
            if(parameter.getMetric() != null){
                termsBuilder.subAggregation(getMetricsAggregationBuilder(parameter.getMetric()));
            }
            query.setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                    .genFilter(parameter.getFilters()))
                    .setSearchType(SearchType.COUNT)
                    .addAggregation(rootBuilder);
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
            return new GroupResponse(getMap(fields, aggregations,parameter.getMetric()));
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    private AbstractAggregationBuilder getMetricsAggregationBuilder(MetricsAggregation metricsAggregation) {
        MetricsAggragationType type = metricsAggregation.getType();
        String field = metricsAggregation.getField();
        switch (type){
            case sum:
                return AggregationBuilders.sum(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            case min:
                return AggregationBuilders.min(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            case max:
                return AggregationBuilders.max(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            case avg:
                return AggregationBuilders.avg(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            case stats:
                return AggregationBuilders.stats(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            case percentiles:
                return AggregationBuilders.percentiles(Utils.sanitizeFieldForAggregation(type.name())).field(field);
            default:
                return null;
        }
    }

    private Map<String, Object> getMap(List<String> fields, Aggregations aggregations, MetricsAggregation metricsAggregation) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1) ? fields.subList(1, fields.size())
                : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        Map<String, Object> levelCount = Maps.newHashMap();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                if(metricsAggregation != null){
                    levelCount.put(bucket.getKey(), getMetricsAggregationMap(bucket.getDocCount(), bucket.getAggregations(), metricsAggregation));
                }else{
                    levelCount.put(bucket.getKey(), bucket.getDocCount());
                }
            } else {
                levelCount.put(bucket.getKey(), getMap(remainingFields, bucket.getAggregations(),metricsAggregation));
            }
        }
        return levelCount;

    }

    private Map<String, Object> getMetricsAggregationMap(long docCount, Aggregations aggregations, MetricsAggregation metricAggrigation) {
        Map<String,Object> parentMap = Maps.newHashMap();
        parentMap.put("count",docCount);
        MetricsAggragationType type = metricAggrigation.getType();

        Map<String,Object> statsMap = Maps.newHashMap();
        parentMap.put(type.name(),statsMap);

        switch (type){
            case sum:
            case min:
            case max:
            case avg:
                NumericMetricsAggregation.SingleValue sum = aggregations.get(type.name());
                statsMap.put("value",sum.value());
                break;
            case stats:
                Stats stats = aggregations.get(type.name());
                statsMap.put("value", getStatsCountMap(stats));
                break;
            case percentiles:
                Percentiles percentiles = aggregations.get(type.name());
                statsMap.put("value",getPercentileMap(percentiles));
                break;
        }


        return parentMap;
    }

    private Map<Double,Double> getPercentileMap(Percentiles percentiles) {
        Map<Double,Double> statsMap = Maps.newHashMap();
        Iterator<Percentile> iterator = percentiles.iterator();
        while (iterator.hasNext()){
            Percentile percentile = iterator.next();
            statsMap.put(percentile.getPercent(),percentile.getValue());
        }
        return statsMap;
    }

    private Map<String,Object> getStatsCountMap(Stats stats) {
        Map<String,Object> statsMap = Maps.newHashMap();
        statsMap.put("count",stats.getCount());
        statsMap.put("min",stats.getMin());
        statsMap.put("max",stats.getMax());
        statsMap.put("avg",stats.getAvg());
        statsMap.put("sum",stats.getSum());
        return statsMap;
    }

}
