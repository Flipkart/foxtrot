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
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
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
import io.dropwizard.util.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 30/03/14
 * Time: 10:27 PM
 */
@AnalyticsProvider(opcode = "trend", request = TrendRequest.class, response = TrendResponse.class, cacheable = true)
public class TrendAction extends Action<TrendRequest> {
    private static final Logger logger = LoggerFactory.getLogger(TrendAction.class.getSimpleName());

    public TrendAction(TrendRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken);
    }

    @Override
    protected String getRequestCacheKey() {
        TrendRequest query = getParameter();
        long filterHashKey = 0L;
        if (query.getFilters() != null) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }
        if (query.getValues() != null) {
            for (String value : query.getValues()) {
                filterHashKey += 31 * value.hashCode();
            }
        }

        filterHashKey += 31 * query.getPeriod().name().hashCode();
        filterHashKey += 31 * query.getTimestamp().hashCode();
        filterHashKey += 31 * (query.getField() != null ? query.getField().hashCode() : "FIELD".hashCode());

        return String.format("%s-%s-%s-%d", query.getTable(),
                query.getField(), query.getPeriod(), filterHashKey);
    }

    @Override
    public ActionResponse execute(TrendRequest parameter) throws QueryStoreException {
        parameter.setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
        if (parameter.getTable() == null) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid table name");
        }

        if (null == parameter.getFilters()) {
            parameter.setFilters(Lists.<Filter>newArrayList(new AnyFilter(parameter.getTable())));
        }
        String field = parameter.getField();
        if (null == field || field.isEmpty()) {
            throw new QueryStoreException(QueryStoreException.ErrorCode.INVALID_REQUEST, "Invalid field name");
        }
        if (null != parameter.getValues() && parameter.getValues().size() != 0) {
            List<Object> values = (List) parameter.getValues();
            Filter filter = new InFilter(field, values);
            parameter.getFilters().add(filter);
        }

        try {
            AbstractAggregationBuilder aggregationBuilder = buildAggregation(parameter);
            SearchResponse searchResponse = getConnection().getClient()
                    .prepareSearch(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                    .setQuery(new ElasticSearchQueryGenerator(FilterCombinerType.and).genFilter(parameter.getFilters()))
                    .setSearchType(SearchType.COUNT)
                    .addAggregation(aggregationBuilder)
                    .execute()
                    .get();

            Aggregations aggregations = searchResponse.getAggregations();
            if (aggregations != null) {
                return buildResponse(parameter, aggregations);
            } else {
                logger.error("Null response for Trend. Request : " + parameter.toString());
                return new TrendResponse(Collections.<String, List<TrendResponse.Count>>emptyMap());
            }
        } catch (Exception e) {
            logger.error("Error running trend action: ", e);
            throw new QueryStoreException(QueryStoreException.ErrorCode.QUERY_EXECUTION_ERROR,
                    "Error running trend action.", e);
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

    @Override
    protected Filter getDefaultTimeSpan() {
        LastFilter lastFilter = new LastFilter();
        lastFilter.setField("_timestamp");
        lastFilter.setDuration(Duration.days(1));
        return lastFilter;
    }

    private AbstractAggregationBuilder buildAggregation(TrendRequest request) {
        DateHistogram.Interval interval;
        switch (request.getPeriod()) {
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
            default:
                interval = DateHistogram.Interval.HOUR;
                break;
        }

        String field = request.getField();
        return AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field))
                .field(field)
                .size(0)
                .subAggregation(Utils.buildDateHistogramAggregation(request.getTimestamp(), interval));
    }

    private TrendResponse buildResponse(TrendRequest request, Aggregations aggregations){
        String field = request.getField();
        Map<String, List<TrendResponse.Count>> trendCounts = new TreeMap<String, List<TrendResponse.Count>>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        for (Terms.Bucket bucket : terms.getBuckets()) {
            final String key = bucket.getKeyAsText().string();
            List<TrendResponse.Count> counts = Lists.newArrayList();
            Aggregations subAggregations = bucket.getAggregations();
            Histogram histogram = subAggregations.get(Utils.getDateHistogramKey(request.getTimestamp()));
            for (Histogram.Bucket histogramBucket : histogram.getBuckets()) {
                counts.add(new TrendResponse.Count(histogramBucket.getKeyAsNumber(), histogramBucket.getDocCount()));
            }
            trendCounts.put(key, counts);
        }
        return new TrendResponse(trendCounts);
    }
}
