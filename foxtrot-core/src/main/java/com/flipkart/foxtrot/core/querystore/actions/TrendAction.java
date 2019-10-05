/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.util.Duration;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 10:27 PM
 */
@AnalyticsProvider(opcode = "trend", request = TrendRequest.class, response = TrendResponse.class, cacheable = true)
public class TrendAction extends Action<TrendRequest> {

    public TrendAction(TrendRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
    }

    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
        if(null != getParameter().getValues() && !getParameter().getValues()
                .isEmpty()) {
            List<Object> values = (List)getParameter().getValues();
            Filter filter = new InFilter(getParameter().getField(), values);
            getParameter().getFilters()
                    .add(filter);
        }
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public String getRequestCacheKey() {
        TrendRequest query = getParameter();
        long filterHashKey = 0L;
        if(query.getFilters() != null) {
            for(Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        if(query.getValues() != null) {
            for(String value : query.getValues()) {
                filterHashKey += 31 * value.hashCode();
            }
        }

        if(null != query.getUniqueCountOn()) {
            filterHashKey += 31 * query.getUniqueCountOn()
                    .hashCode();
        }


        filterHashKey += 31 * query.getPeriod()
                .name()
                .hashCode();
        filterHashKey += 31 * query.getTimestamp()
                .hashCode();
        filterHashKey += 31 * (query.getField() != null ? query.getField()
                .hashCode() : "FIELD".hashCode());

        return String.format("%s-%s-%s-%d", query.getTable(), query.getField(), query.getPeriod(), filterHashKey);
    }

    @Override
    public void validateImpl(TrendRequest parameter) {
        List<String> validationErrors = Lists.newArrayList();
        if(CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if(CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if(CollectionUtils.isNullOrEmpty(parameter.getTimestamp())) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if(parameter.getPeriod() == null) {
            validationErrors.add(String.format("specify time period (%s)", StringUtils.join(Period.values())));
        }

        if(parameter.getUniqueCountOn() != null && parameter.getUniqueCountOn()
                .isEmpty()) {
            validationErrors.add("unique field cannot be empty (can be null)");
        }

        if(!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(TrendRequest parameter) {
        SearchRequestBuilder searchRequestBuilder = getRequestBuilder(parameter);
        try {
            SearchResponse searchResponse = searchRequestBuilder.execute()
                    .actionGet(getGetQueryTimeout());
            return getResponse(searchResponse, parameter);
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequestBuilder getRequestBuilder(TrendRequest parameter) {
        SearchRequestBuilder searchRequestBuilder;
        try {
            AbstractAggregationBuilder aggregationBuilder = buildAggregation(parameter);
            searchRequestBuilder = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator().genFilter(parameter.getFilters()))
                    .setSize(QUERY_SIZE)
                    .addAggregation(aggregationBuilder);
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        return searchRequestBuilder;
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, TrendRequest parameter) {
        Aggregations aggregations = ((SearchResponse)response).getAggregations();
        if(aggregations != null) {
            return buildResponse(parameter, aggregations);
        } else {
            return new TrendResponse(Collections.<String, List<TrendResponse.Count>>emptyMap());
        }
    }


    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

    private AbstractAggregationBuilder buildAggregation(TrendRequest request) {
        DateHistogramInterval interval = Utils.getHistogramInterval(request.getPeriod());
        String field = request.getField();

        DateHistogramAggregationBuilder histogramBuilder = Utils.buildDateHistogramAggregation(request.getTimestamp(), interval);
        if(!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
            histogramBuilder.subAggregation(Utils.buildCardinalityAggregation(getParameter().getUniqueCountOn()));
        }
        return Utils.buildTermsAggregation(Lists.newArrayList(new ResultSort(field, ResultSort.Order.asc)),
                                           Sets.newHashSet(histogramBuilder)
                                          );
    }

    private TrendResponse buildResponse(TrendRequest request, Aggregations aggregations) {
        String field = request.getField();
        Map<String, List<TrendResponse.Count>> trendCounts = new TreeMap<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        for(Terms.Bucket bucket : terms.getBuckets()) {
            final String key = String.valueOf(bucket.getKey());
            List<TrendResponse.Count> counts = Lists.newArrayList();
            Aggregations subAggregations = bucket.getAggregations();
            Histogram histogram = subAggregations.get(Utils.getDateHistogramKey(request.getTimestamp()));
            for(Histogram.Bucket histogramBucket : histogram.getBuckets()) {
                if(!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
                    String uniqueCountKey = Utils.sanitizeFieldForAggregation(getParameter().getUniqueCountOn());
                    Cardinality cardinality = histogramBucket.getAggregations()
                            .get(uniqueCountKey);
                    counts.add(new TrendResponse.Count(((DateTime)histogramBucket.getKey()).getMillis(), cardinality.getValue()));
                } else {
                    counts.add(new TrendResponse.Count(((DateTime)histogramBucket.getKey()).getMillis(), histogramBucket.getDocCount()));
                }
            }
            trendCounts.put(key, counts);
        }
        return new TrendResponse(trendCounts);
    }
}
