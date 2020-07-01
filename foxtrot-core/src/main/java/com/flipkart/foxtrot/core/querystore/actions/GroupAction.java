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

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.visitor.CountPrecisionThresholdVisitorAdapter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 7:16 PM
 */
@AnalyticsProvider(opcode = "group", request = GroupRequest.class, response = GroupResponse.class, cacheable = true)
@Slf4j
@SuppressWarnings("squid:CallToDeprecatedMethod")
public class GroupAction extends Action<GroupRequest> {

    private static final long MAX_CARDINALITY = 50000;
    private static final long MIN_ESTIMATION_THRESHOLD = 1000;
    private static final double PROBABILITY_CUT_OFF = 0.5;

    private final ElasticsearchTuningConfig elasticsearchTuningConfig;

    public GroupAction(GroupRequest parameter,
                       AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.elasticsearchTuningConfig = analyticsLoader.getElasticsearchTuningConfig();
    }

    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public String getRequestCacheKey() {
        long filterHashKey = 0L;
        GroupRequest query = getParameter();
        if (null != query.getFilters()) {
            for (Filter filter : query.getFilters()) {
                filterHashKey += 31 * (Integer) filter.accept(getCacheKeyVisitor());
            }
        }
        if (null != query.getUniqueCountOn()) {
            filterHashKey += 31 * query.getUniqueCountOn()
                    .hashCode();
        }

        if (null != query.getAggregationField()) {
            filterHashKey += 31 * query.getAggregationField()
                    .hashCode();
        }

        if (!CollectionUtils.isNullOrEmpty(query.getStats())) {
            for (Stat stat : query.getStats()) {
                filterHashKey += 31 * stat.hashCode();
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
    public void validateImpl(GroupRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }

        if (CollectionUtils.isNullOrEmpty(parameter.getNesting())) {
            validationErrors.add("at least one grouping parameter is required");
        } else {
            validationErrors.addAll(parameter.getNesting()
                    .stream()
                    .filter(CollectionUtils::isNullOrEmpty)
                    .map(field -> "grouping parameter cannot have null or empty name")
                    .collect(Collectors.toList()));
        }

        if (parameter.getUniqueCountOn() != null && parameter.getUniqueCountOn()
                .isEmpty()) {
            validationErrors.add("uniqueCountOn cannot be empty (can be null)");
        }

        if (parameter.getAggregationField() != null && parameter.getAggregationField()
                .isEmpty()) {
            validationErrors.add("aggregation field cannot be empty (can be null)");
        }

        getCardinalityValidator().validateCardinality(this, parameter, parameter.getTable(), parameter.getNesting());

        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }


    }


    @Override
    public ActionResponse execute(GroupRequest parameter) {
        SearchRequest query = getRequestBuilder(parameter, Collections.emptyList());
        try {
            SearchResponse response = getConnection().getClient()
                    .search(query);
            return getResponse(response, parameter);
        } catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchRequest getRequestBuilder(GroupRequest parameter,
                                           List<Filter> extraFilters) {
        return new SearchRequest(ElasticsearchUtils.getIndices(parameter.getTable(), parameter))
                .indicesOptions(Utils.indicesOptions())
                .source(new SearchSourceBuilder().size(QUERY_SIZE)
                        .timeout(new TimeValue(getGetQueryTimeout(), TimeUnit.MILLISECONDS))
                        .query(ElasticsearchQueryUtils.translateFilter(parameter, extraFilters))
                        .aggregation(buildAggregation(parameter)));
    }

    @Override
    public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response,
                                      GroupRequest parameter) {
        List<String> fields = parameter.getNesting();
        Aggregations aggregations = ((SearchResponse) response).getAggregations();
        // Check if any aggregation is present or not
        if (aggregations == null) {
            return new GroupResponse(Collections.emptyMap());
        }
        return new GroupResponse(getMap(fields, aggregations));
    }

    private AbstractAggregationBuilder buildAggregation(GroupRequest parameter) {
        return Utils.buildTermsAggregation(getParameter().getNesting()
                        .stream()
                        .map(x -> new ResultSort(x, ResultSort.Order.asc))
                        .collect(Collectors.toList()), buildSubAggregation(getParameter()),
                elasticsearchTuningConfig.getAggregationSize());

    }

    private Set<AggregationBuilder> buildSubAggregation(GroupRequest groupRequest) {
        // Keep this for backward compatibility to support uniqueCountOn attribute coming from old requests
        if (!Strings.isNullOrEmpty(groupRequest.getUniqueCountOn())) {
            return Sets.newHashSet(buildCardinalityAggregation(groupRequest.getUniqueCountOn(), groupRequest));
        }

        if (Strings.isNullOrEmpty(groupRequest.getAggregationField())) {
            return Sets.newHashSet();
        }

        boolean isNumericField = Utils.isNumericField(getTableMetadataManager(), groupRequest.getTable(),
                groupRequest.getAggregationField());
        final AbstractAggregationBuilder groupAggStats;
        if (isNumericField) {
            groupAggStats = Utils.buildStatsAggregation(groupRequest.getAggregationField(), groupRequest.getStats());
        } else {
            groupAggStats = buildCardinalityAggregation(groupRequest.getAggregationField(), groupRequest);
        }
        return Sets.newHashSet(groupAggStats);
    }

    private CardinalityAggregationBuilder buildCardinalityAggregation(String aggregationField,
                                                                      GroupRequest groupRequest) {
        return Utils.buildCardinalityAggregation(aggregationField, groupRequest.accept(
                new CountPrecisionThresholdVisitorAdapter(elasticsearchTuningConfig.getPrecisionThreshold())));
    }

    private Map<String, Object> getMap(List<String> fields,
                                       Aggregations aggregations) {
        final String field = fields.get(0);
        final List<String> remainingFields = (fields.size() > 1)
                                             ? fields.subList(1, fields.size())
                                             : new ArrayList<>();
        Terms terms = aggregations.get(Utils.sanitizeFieldForAggregation(field));
        Map<String, Object> levelCount = Maps.newHashMap();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            if (fields.size() == 1) {
                if (!Strings.isNullOrEmpty(getParameter().getUniqueCountOn())) {
                    String key = Utils.sanitizeFieldForAggregation(getParameter().getUniqueCountOn());
                    Cardinality cardinality = bucket.getAggregations()
                            .get(key);
                    levelCount.put(String.valueOf(bucket.getKey()), cardinality.getValue());
                } else if (!Strings.isNullOrEmpty(getParameter().getAggregationField())) {
                    String metricKey = Utils.getExtendedStatsAggregationKey(getParameter().getAggregationField());
                    levelCount.put(String.valueOf(bucket.getKey()), Utils.toStats(bucket.getAggregations()
                            .getAsMap()
                            .get(metricKey)));
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
