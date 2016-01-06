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
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import com.yammer.dropwizard.util.Duration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 29/03/14
 * Time: 9:22 PM
 */
@AnalyticsProvider(opcode = "histogram", request = HistogramRequest.class, response = HistogramResponse.class, cacheable = true)
public class HistogramAction extends Action<HistogramRequest> {

    public HistogramAction(HistogramRequest parameter,
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
        HistogramRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }

        return String.format("%s-%s-%s-%d", query.getTable(),
                query.getPeriod().name(), query.getField(), filterHashKey);
    }

    @Override
    public ActionResponse execute(HistogramRequest parameter) throws FoxtrotException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(parameter.getTable()));
        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }

        if (parameter.getField() == null || parameter.getField().trim().isEmpty()) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, "field cannot be null");
        }

        SearchRequestBuilder searchRequestBuilder;
        DateHistogram.Interval interval = null;
        switch (parameter.getPeriod()) {
            case seconds:
                interval = DateHistogram.Interval.SECOND;
                break;
            case minutes:
                interval = DateHistogram.Interval.MINUTE;
                break;
            case hours:
                interval = DateHistogram.Interval.HOUR;
                break;
            case days:
                interval = DateHistogram.Interval.DAY;
                break;
        }
        String dateHistogramKey = Utils.sanitizeFieldForAggregation(parameter.getField());
        try {
            searchRequestBuilder = getConnection().getClient().prepareSearch(
                    ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setTypes(ElasticsearchUtils.DOCUMENT_TYPE_NAME)
                    .setIndicesOptions(Utils.indicesOptions())
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and)
                            .genFilter(parameter.getFilters()))
                    .setSize(0)
                    .setSearchType(SearchType.COUNT)
                    .addAggregation(AggregationBuilders.dateHistogram(dateHistogramKey)
                            .field(parameter.getField())
                            .interval(interval));
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }

        try {
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            Aggregations aggregations = response.getAggregations();
            if (aggregations == null) {
                return new HistogramResponse(Collections.<HistogramResponse.Count>emptyList());
            }
            DateHistogram dateHistogram = aggregations.get(dateHistogramKey);
            Collection<? extends DateHistogram.Bucket> buckets = dateHistogram.getBuckets();
            List<HistogramResponse.Count> counts = new ArrayList<>(buckets.size());
            for (DateHistogram.Bucket bucket : buckets) {
                HistogramResponse.Count count = new HistogramResponse.Count(
                        bucket.getKeyAsNumber(), bucket.getDocCount());
                counts.add(count);
            }
            return new HistogramResponse(counts);
        } catch (ElasticsearchException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

}
