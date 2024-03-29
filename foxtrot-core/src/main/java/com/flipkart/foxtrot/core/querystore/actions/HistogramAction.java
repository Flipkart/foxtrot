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
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.visitor.CountPrecisionThresholdVisitorAdapter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.config.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import io.dropwizard.util.Duration;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 29/03/14
 * Time: 9:22 PM
 */
@AnalyticsProvider(opcode = "histogram", request = HistogramRequest.class, response = HistogramResponse.class, cacheable = true)
public class HistogramAction extends Action<HistogramRequest> {

    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    public HistogramAction(HistogramRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.elasticsearchTuningConfig = analyticsLoader.getElasticsearchTuningConfig();
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public String getRequestCacheKey() {
        long filterHashKey = 0L;
        HistogramRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }

        if (null != query.getUniqueCountOn()) {
            filterHashKey += 31 * query.getUniqueCountOn()
                    .hashCode();
        }


        return String.format("%s-%s-%s-%d", query.getTable(), query.getPeriod()
                .name(), query.getField(), filterHashKey);
    }

    @Override
    public void validateImpl(HistogramRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if (parameter.getPeriod() == null) {
            validationErrors.add("time period cannot be null");
        }

        if (parameter.getUniqueCountOn() != null && parameter.getUniqueCountOn()
                .isEmpty()) {
            validationErrors.add("distinct field cannot be empty (can be null)");
        }

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(HistogramRequest parameter) {
        SearchRequest query = getRequestBuilder(parameter, Collections.emptyList());
        try {
            SearchResponse response = getConnection()
                    .getClient()
                    .search(query);
            return getResponse(response, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequest getRequestBuilder(HistogramRequest parameter, List<Filter> extraFilters) {
        return new SearchRequest(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                .indicesOptions(Utils.indicesOptions())
                .source(new SearchSourceBuilder()
                        .size(QUERY_SIZE)
                        .timeout(new TimeValue(getGetQueryTimeout(), TimeUnit.MILLISECONDS))
                        .query(ElasticsearchQueryUtils.translateFilter(parameter, extraFilters))
                        .aggregation(buildAggregation(parameter)));
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, HistogramRequest parameter) {
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        return buildResponse(aggregations);
    }

    private HistogramResponse buildResponse(Aggregations aggregations) {
        if (aggregations == null) {
            return new HistogramResponse(Collections.<HistogramResponse.Count>emptyList());
        }


        String dateHistogramKey = Utils.getDateHistogramKey(getParameter().getField());
        Histogram dateHistogram = aggregations.get(dateHistogramKey);
        Collection<? extends Histogram.Bucket> buckets = dateHistogram.getBuckets();
        List<HistogramResponse.Count> counts = new ArrayList<>(buckets.size());
        for (Histogram.Bucket bucket : buckets) {
            if (!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
                String key = Utils.sanitizeFieldForAggregation(getParameter().getUniqueCountOn());
                Cardinality cardinality = bucket.getAggregations()
                        .get(key);
                counts.add(new HistogramResponse.Count(((DateTime) bucket.getKey()).getMillis(),
                        cardinality.getValue()));
            } else {
                counts.add(new HistogramResponse.Count(((DateTime) bucket.getKey()).getMillis(), bucket.getDocCount()));
            }
        }
        return new HistogramResponse(counts);
    }

    private AbstractAggregationBuilder buildAggregation(HistogramRequest parameter) {
        DateHistogramInterval interval = Utils.getHistogramInterval(getParameter().getPeriod());
        DateHistogramAggregationBuilder histogramBuilder = Utils.buildDateHistogramAggregation(getParameter().getField(),
                interval);
        if (!CollectionUtils.isNullOrEmpty(getParameter().getUniqueCountOn())) {
            histogramBuilder.subAggregation(Utils.buildCardinalityAggregation(
                    getParameter().getUniqueCountOn(), parameter.accept(new CountPrecisionThresholdVisitorAdapter(
                            elasticsearchTuningConfig.getPrecisionThreshold()))));
        }
        return histogramBuilder;
    }

    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

}
