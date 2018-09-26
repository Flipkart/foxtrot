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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.estimation.*;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitorAdapter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotInFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.exception.MalformedQueryException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.query.ElasticSearchQueryGenerator;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.joda.time.Interval;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 7:16 PM
 */
@AnalyticsProvider(opcode = "group", request = GroupRequest.class, response = GroupResponse.class, cacheable = true)
@Slf4j
public class GroupAction extends Action<GroupRequest> {

    private static final long MAX_CARDINALITY = 50000;
    private static final long MIN_ESTIMATION_THRESHOLD = 1000;
    private static final double PROBABILITY_CUT_OFF = 0.5;

    public GroupAction(GroupRequest parameter,
                       TableMetadataManager tableMetadataManager,
                       DataStore dataStore,
                       QueryStore queryStore,
                       ElasticsearchConnection connection,
                       String cacheToken,
                       CacheManager cacheManager, ObjectMapper objectMapper) {
        super(parameter, tableMetadataManager, dataStore, queryStore, connection, cacheToken, cacheManager, objectMapper);
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

        // Perform cardinality analysis and see how much this fucks up the cluster
        QueryStore queryStore = getQueryStore();
        if(queryStore instanceof ElasticsearchQueryStore && ((ElasticsearchQueryStore)queryStore).getCardinalityConfig().isEnabled()) {
            double probability = 0;
            try {
                TableFieldMapping fieldMappings = getTableMetadataManager().getFieldMappings(parameter.getTable(), true, false);
                if (null == fieldMappings) {
                    fieldMappings = TableFieldMapping.builder()
                            .mappings(Collections.emptySet())
                            .table(parameter.getTable())
                            .build();
                }

                probability = estimateProbability(fieldMappings, parameter);
            } catch (Exception e) {
                log.error("Error running estimation", e);
            }

            if (probability > PROBABILITY_CUT_OFF) {
                try {
                    log.warn("Blocked query as it might have screwed up the cluster. Probability: {} Query: {}",
                            probability, getObjectMapper().writeValueAsString(parameter));
                } catch (JsonProcessingException e) {
                    log.warn("Blocked query as it might have screwed up the cluster. Probability: {} Query: {}",
                            probability, parameter);
                }
                throw FoxtrotExceptions.createCardinalityOverflow(parameter, parameter.getNesting().get(0), probability);
            } else {
                log.info("Allowing group by with probability {} for query: {}", probability, parameter);
            }
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
            query.setQuery(new ElasticSearchQueryGenerator()
                    .genFilter(parameter.getFilters()))
                    .setSize(0)
                    .addAggregation(aggregation);
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
        try {
            SearchResponse response = query.execute().actionGet(getGetQueryTimeout());
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

    private double estimateProbability(TableFieldMapping tableFieldMapping, GroupRequest parameter) throws Exception {
        Set<FieldMetadata> mappings = tableFieldMapping.getMappings();
        Map<String, FieldMetadata> metaMap = mappings.stream()
                .collect(Collectors.toMap(FieldMetadata::getField, mapping -> mapping));

        String cacheKey = getRequestCacheKey();
        long estimatedMaxDocCount = extractMaxDocCount(metaMap);
        log.debug("cacheKey:{} msg:DOC_COUNT_ESTIMATION_COMPLETED maxDocCount:{}",
                cacheKey, estimatedMaxDocCount);
        long estimatedDocCountBasedOnTime = estimateDocCountBasedOnTime(estimatedMaxDocCount, parameter,
                getTableMetadataManager(), tableFieldMapping.getTable());
        log.debug("cacheKey:{} msg:TIME_BASED_DOC_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{}",
                cacheKey, estimatedMaxDocCount, estimatedDocCountBasedOnTime);
        long estimatedDocCountAfterFilters = estimateDocCountWithFilters(estimatedDocCountBasedOnTime, metaMap, parameter.getFilters());
        log.debug("cacheKey:{} msg:ALL_FILTER_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{} docCountAfterFilters:{}",
                cacheKey, estimatedMaxDocCount, estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters);
        if (estimatedDocCountAfterFilters < MIN_ESTIMATION_THRESHOLD) {
            log.debug("cacheKey:{} msg:NESTING_ESTIMATION_SKIPPED estimatedDocCount:{} threshold:{}",
                    cacheKey, estimatedDocCountAfterFilters, MIN_ESTIMATION_THRESHOLD);
            return 0.0;
        }

        long outputCardinality = 1;
        final AtomicBoolean reduceCardinality = new AtomicBoolean(false);
        for (int i = 0; i < parameter.getNesting().size(); i++) {
            final String field = parameter.getNesting().get(i);
            FieldMetadata metadata = metaMap.get(field);
            if (null == metadata || null == metadata.getEstimationData()) {
                log.warn("cacheKey:{} msg:NO_FIELD_ESTIMATION_DATA table:{} field:{}", cacheKey, parameter.getTable(), field);
                continue;
            }
            long fieldCardinality = metadata.getEstimationData().accept(new EstimationDataVisitor<Long>() {
                @Override
                public Long visit(FixedEstimationData fixedEstimationData) {
                    return fixedEstimationData.getCount();
                }

                @Override
                public Long visit(PercentileEstimationData percentileEstimationData) {
                    reduceCardinality.getAndSet(true);
                    return percentileEstimationData.getCardinality();
                }

                @Override
                public Long visit(CardinalityEstimationData cardinalityEstimationData) {
                    return (cardinalityEstimationData.getCardinality() * estimatedDocCountAfterFilters) / cardinalityEstimationData.getCount();
                }

                @Override
                public Long visit(TermHistogramEstimationData termEstimationData) {
                    reduceCardinality.getAndSet(true);
                    return (long) termEstimationData.getTermCounts().size();
                }
            });
            log.debug("cacheKey:{} msg:NESTING_FIELD_ESTIMATED field:{} overallCardinality:{} fieldCardinality:{} newCardinality:{}",
                    cacheKey, field, outputCardinality, fieldCardinality, outputCardinality * fieldCardinality);
            /*if (fieldCardinality != 0) {
                fieldCardinality = (long) Utils.ensureOne((long) Math.pow(Math.abs(fieldCardinality), 1 / Math.pow(2, i + 1)));
            }*/
            fieldCardinality = (long) Utils.ensureOne(fieldCardinality);
            log.debug("cacheKey:{} msg:NESTING_FIELD_ESTIMATION_COMPLETED field:{} overallCardinality:{} fieldCardinality:{} newCardinality:{}",
                    cacheKey, field, outputCardinality, fieldCardinality, outputCardinality * fieldCardinality);
            outputCardinality *= fieldCardinality;
        }

        //Although cardinality will not be reduced by the same factor as documents count reduced.
        //To give benefit of doubt or if someone is making query on a smaller time frame using fields of higher cardinality, reducing cardinality for that query
        //Only reducing cardinality if the doc count is actually less than docCount for a day. Assuming cardinality will remain same if query for more than 1 day
        if (((double) estimatedDocCountAfterFilters / estimatedMaxDocCount) < 1.0 && reduceCardinality.get()) {
            outputCardinality = (long) (outputCardinality * ((double) estimatedDocCountAfterFilters / estimatedMaxDocCount));
        }

        log.debug("cacheKey:{} msg:NESTING_FIELDS_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{} docCountAfterFilters:{} outputCardinality:{}",
                cacheKey, estimatedMaxDocCount, estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters, outputCardinality);
        long maxCardinality = MAX_CARDINALITY;
        if (getQueryStore() instanceof ElasticsearchQueryStore && ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig() != null
                && ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig().getMaxCardinality() != 0) {
            maxCardinality = ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig().getMaxCardinality();
        }
        if (outputCardinality > maxCardinality) {
            log.warn("Output cardinality : {}, estimatedMaxDocCount : {}, estimatedDocCountBasedOnTime : {}, " +
                            "estimatedDocCountAfterFilters : {}, TableFieldMapping : {},  Query: {}", outputCardinality,
                    estimatedMaxDocCount, estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters, tableFieldMapping,
                    getObjectMapper().writeValueAsString(parameter));
            return 1.0;
        } else {
            return 0;
        }
    }

    private long estimateDocCountBasedOnTime(long currentDocCount, GroupRequest parameter,
                                             TableMetadataManager tableMetadataManager, String table) throws Exception {
        Interval queryInterval = new PeriodSelector(parameter.getFilters()).analyze();
        long minutes = queryInterval.toDuration().getStandardMinutes();
        double days = (double) minutes / TimeUnit.DAYS.toMinutes(1);
        Table tableMetadata = tableMetadataManager.get(table);
        int maxDays = 0;
        if (tableMetadata != null) {
            maxDays = tableMetadata.getTtl();
        }
        //If we don't have it in metadata, assuming max data of 30 days
        if (maxDays == 0) {
            maxDays = 30;
        }
        //This is done because we only store docs for last maxDays. Sometimes, we get startTime starting from 1970 year
        if (days > maxDays) {
            return currentDocCount * maxDays;
        } else {
            return (long) (currentDocCount * days);
        }
    }

    private long extractMaxDocCount(Map<String, FieldMetadata> metaMap) {
        return metaMap.values().stream()
                .map(x -> x.getEstimationData() == null ? 0 : x.getEstimationData().getCount())
                .max(Comparator.naturalOrder())
                .orElse((long) 0);
    }

    private long estimateDocCountWithFilters(long currentDocCount,
                                             Map<String, FieldMetadata> metaMap,
                                             List<Filter> filters) throws Exception {
        if (CollectionUtils.isNullOrEmpty(filters)) {
            return currentDocCount;
        }

        String cacheKey = getRequestCacheKey();

        double overallFilterMultiplier = 1;
        for (Filter filter : filters) {
            final String filterField = filter.getField();
            FieldMetadata fieldMetadata = metaMap.get(filterField);
            if (null == fieldMetadata || null == fieldMetadata.getEstimationData()) {
                log.warn("cacheKey:{} msg:NO_FIELD_ESTIMATION_DATA field:{}", cacheKey, filterField);
                continue;
            }
            log.debug("cacheKey:{} msg:FILTER_ESTIMATION_STARTED filter:{} mapping:{}", cacheKey, filter, fieldMetadata);
            double currentFilterMultiplier = fieldMetadata.getEstimationData()
                    .accept(new EstimationDataVisitor<Double>() {
                        @Override
                        @SneakyThrows
                        public Double visit(FixedEstimationData fixedEstimationData) {
                            return filter.accept(new FilterVisitorAdapter<Double>(1.0) {

                                @Override
                                public Double visit(EqualsFilter equalsFilter) throws Exception {
                                    //If there is a match it will be atmost one out of all the values present
                                    return 1.0 / Utils.ensureOne(fixedEstimationData.getCount());
                                }

                                @Override
                                public Double visit(NotEqualsFilter notEqualsFilter) throws Exception {
                                    // Assuming a match, there will be N-1 unmatched values
                                    double numerator = Utils.ensurePositive(fixedEstimationData.getCount() - 1);
                                    return numerator / Utils.ensureOne(fixedEstimationData.getCount());

                                }

                                @Override
                                public Double visit(ContainsFilter stringContainsFilterElement) throws Exception {
                                    // Assuming there is a match to a value.
                                    // Can be more, but we err on the side of optimism.
                                    return (1.0 / Utils.ensureOne(fixedEstimationData.getCount()));

                                }

                                @Override
                                public Double visit(InFilter inFilter) throws Exception {
                                    // Assuming there are M matches, the probability is M/N
                                    return Utils.ensurePositive(inFilter.getValues().size())
                                            / Utils.ensureOne(fixedEstimationData.getCount());
                                }

                                @Override
                                public Double visit(NotInFilter notInFilter) throws Exception {
                                    // Assuming there are M matches, then probability will be N - M / N
                                    return Utils.ensurePositive(fixedEstimationData.getCount() - notInFilter.getValues().size())
                                            / Utils.ensureOne(fixedEstimationData.getCount());
                                }
                            });
                        }

                        @Override
                        @SneakyThrows
                        public Double visit(PercentileEstimationData percentileEstimationData) {
                            final double[] percentiles = percentileEstimationData.getValues();
                            final long numMatches = percentileEstimationData.getCount();
                            return filter.accept(new FilterVisitorAdapter<Double>(1.0) {
                                @Override
                                public Double visit(BetweenFilter betweenFilter) throws Exception {

                                    //What percentage percentiles are >= above lower bound
                                    int minBound = IntStream.rangeClosed(0, 9)
                                            .filter(i -> betweenFilter.getFrom().doubleValue()
                                                    <= percentiles[i])
                                            .findFirst()
                                            .orElse(0);
                                    // What percentage of values are > upper bound
                                    int maxBound = IntStream.rangeClosed(0, 9)
                                            .filter(i -> betweenFilter.getTo().doubleValue()
                                                    < percentiles[i])
                                            .findFirst()
                                            .orElse(9);

                                    int numBuckets = maxBound - minBound + 1;
                                    final double result = (double) numBuckets / 10.0;
                                    log.debug("cacheKey:{} Between filter: {} " +
                                                    "percentiles[{}] = {} to percentiles[{}] = {} " +
                                                    "buckets {} multiplier {}",
                                            cacheKey,
                                            betweenFilter,
                                            minBound,
                                            percentiles[minBound],
                                            maxBound,
                                            percentiles[maxBound],
                                            numBuckets,
                                            result);
                                    return result;
                                }

                                @Override
                                public Double visit(EqualsFilter equalsFilter) throws Exception {
                                    Long value = (Long) equalsFilter.getValue();
                                    //What percentage percentiles are >= above lower bound
                                    int minBound = IntStream.rangeClosed(0, 9)
                                            .filter(i -> value <= percentiles[i])
                                            .findFirst()
                                            .orElse(0);
                                    // What percentage of values are > upper bound
                                    int maxBound = IntStream.rangeClosed(0, 9)
                                            .filter(i -> value < percentiles[i])
                                            .findFirst()
                                            .orElse(9);
                                    int numBuckets = maxBound - minBound + 1;
                                    final double result = (double) numBuckets / 10.0;
                                    log.debug("cacheKey:{} EqualsFilter:{} numMatches:{} multiplier:{}",
                                            cacheKey, equalsFilter, numMatches, result);
                                    return result;
                                }

                                @Override
                                public Double visit(NotEqualsFilter notEqualsFilter) throws Exception {
                                    // There is no match, so all values will be considered
                                    log.debug("cacheKey:{} NotEqualsFilter:{} multiplier: 1.0", cacheKey, notEqualsFilter);
                                    return 1.0;
                                }

                                @Override
                                public Double visit(GreaterThanFilter greaterThanFilter) throws Exception {
                                    //Percentage of values greater than given value
                                    //Found when we find a percentile value > bound
                                    int minBound = IntStream.rangeClosed(0, 9)
                                            .filter(i ->
                                                    percentiles[i] > greaterThanFilter.getValue().doubleValue())
                                            //Stop when we find a value
                                            .findFirst()
                                            .orElse(0);

                                    //Everything below this percentile do not affect
                                    final double result = (double) (10 - minBound - 1) / 10.0;
                                    log.debug("cacheKey:{} GreaterThanFilter: {} percentiles[{}] = {} multiplier: {}",
                                            cacheKey,
                                            greaterThanFilter,
                                            minBound,
                                            percentiles[minBound],
                                            result);
                                    return result;
                                }

                                @Override
                                public Double visit(GreaterEqualFilter greaterEqualFilter) throws Exception {
                                    //Percentage of values greater than or equal to given value
                                    //Found when we find a percentile value > bound
                                    int minBound = IntStream.rangeClosed(0, 9)
                                            .filter(i ->
                                                    percentiles[i] >= greaterEqualFilter.getValue().doubleValue())
                                            //Stop when we find a value >= bound
                                            .findFirst()
                                            .orElse(0);

                                    //Everything below this do not affect
                                    final double result = (double) (10 - minBound - 1) / 10.0;
                                    log.debug("cacheKey:{} GreaterEqualsFilter:{} percentiles[{}] = {} multiplier: {}",
                                            cacheKey,
                                            greaterEqualFilter,
                                            minBound,
                                            percentiles[minBound],
                                            result);
                                    return result;
                                }

                                @Override
                                public Double visit(LessThanFilter lessThanFilter) throws Exception {
                                    //Percentage of values lesser than to bound
                                    //Found when we find a percentile value >= bound
                                    int minBound = 9 - IntStream.rangeClosed(0, 9)
                                            .filter(i ->
                                                    percentiles[9 - i] < lessThanFilter.getValue().doubleValue())
                                            //Stop when we find a value >= bound
                                            .findFirst()
                                            .orElse(0);

                                    //Everything above this do not affect
                                    final double result = ((double) minBound + 1.0) / 10.0;
                                    log.debug("cacheKey:{} LessThanFilter:{} percentiles[{}] = {} multiplier: {}",
                                            cacheKey,
                                            lessThanFilter,
                                            minBound,
                                            percentiles[minBound],
                                            result);
                                    return result;
                                }

                                @Override
                                public Double visit(LessEqualFilter lessEqualFilter) throws Exception {
                                    //Percentage of values lesser than or equal to bound
                                    //Found when we find a percentile value > bound
                                    int minBound = 9 - IntStream.rangeClosed(0, 9)
                                            .filter(i ->
                                                    percentiles[9 - i] <= lessEqualFilter.getValue().doubleValue())
                                            //Stop when we find a value > bound
                                            .findFirst()
                                            .orElse(0);
                                    //Everything above this do not affect
                                    final double result = ((double) minBound + 1.0) / 10.0;
                                    log.debug("cacheKey:{} LessEqualsFilter: {} percentiles[{}] = {} multiplier: {}",
                                            cacheKey,
                                            lessEqualFilter,
                                            minBound,
                                            percentiles[minBound],
                                            result);
                                    return result;
                                }
                            });
                        }

                        @Override
                        @SneakyThrows
                        public Double visit(CardinalityEstimationData cardinalityEstimationData) {
                            return filter.accept(new FilterVisitorAdapter<Double>(1.0) {

                                @Override
                                public Double visit(EqualsFilter equalsFilter) throws Exception {
                                    //If there is a match it will be atmost one out of all the values present
                                    return 1.0 / Utils.ensureOne(cardinalityEstimationData.getCardinality());
                                }

                                @Override
                                public Double visit(NotEqualsFilter notEqualsFilter) throws Exception {
                                    // Assuming a match, there will be N-1 unmatched values
                                    double numerator = Utils.ensurePositive(cardinalityEstimationData.getCardinality() - 1);
                                    return numerator / Utils.ensureOne(cardinalityEstimationData.getCardinality());

                                }

                                @Override
                                public Double visit(ContainsFilter stringContainsFilterElement) throws Exception {
                                    // Assuming there is a match to a value.
                                    // Can be more, but we err on the side of optimism.
                                    return (1.0 / Utils.ensureOne(cardinalityEstimationData.getCardinality()));

                                }

                                @Override
                                public Double visit(InFilter inFilter) throws Exception {
                                    // Assuming there are M matches, the probability is M/N
                                    return Utils.ensurePositive(inFilter.getValues().size())
                                            / Utils.ensureOne(cardinalityEstimationData.getCardinality());
                                }

                                @Override
                                public Double visit(NotInFilter notInFilter) throws Exception {
                                    // Assuming there are M matches, then probability will be N - M / N
                                    return Utils.ensurePositive(
                                            cardinalityEstimationData.getCardinality()
                                                    - notInFilter.getValues().size())
                                            / Utils.ensureOne(cardinalityEstimationData.getCardinality());
                                }
                            });
                        }

                        @Override
                        @SneakyThrows
                        public Double visit(TermHistogramEstimationData termEstimationData) {
                            long totalCount = termEstimationData.getCount();
                            return filter.accept(new FilterVisitorAdapter<Double>(1.0) {
                                @Override
                                public Double visit(EqualsFilter equalsFilter) throws Exception {
                                    if (!(equalsFilter.getValue() instanceof String)
                                            || !termEstimationData.getTermCounts().containsKey(equalsFilter.getValue())) {
                                        return 1.0;
                                    }
                                    long matchingDocCount = termEstimationData.getTermCounts().get(equalsFilter.getValue());
                                    return (double) matchingDocCount / totalCount;
                                }

                                @Override
                                public Double visit(NotEqualsFilter notEqualsFilter) throws Exception {
                                    if (!(notEqualsFilter.getValue() instanceof String)
                                            || !termEstimationData.getTermCounts().containsKey(notEqualsFilter.getValue())) {
                                        return 1.0;
                                    }
                                    long matchingDocCount = termEstimationData.getTermCounts().get(notEqualsFilter.getValue());
                                    return (double) (totalCount - matchingDocCount) / totalCount;
                                }

                                @Override
                                public Double visit(InFilter inFilter) throws Exception {
                                    for (Object value : inFilter.getValues()) {
                                        if (!(value instanceof String)) {
                                            return 1.0;
                                        }
                                    }

                                    long matchingDocCount = 0;
                                    for (Object value : inFilter.getValues()) {
                                        Long count = termEstimationData.getTermCounts().get(value);
                                        matchingDocCount += count == null ? 0 : count;
                                    }
                                    return (double) (matchingDocCount) / totalCount;
                                }

                                @Override
                                public Double visit(NotInFilter notInFilter) throws Exception {
                                    for (Object value : notInFilter.getValues()) {
                                        if (!(value instanceof String)) {
                                            return 1.0;
                                        }
                                    }

                                    long matchingDocCount = 0;
                                    for (Object value : notInFilter.getValues()) {
                                        Long count = termEstimationData.getTermCounts().get(value);
                                        matchingDocCount += count == null ? 0 : count;
                                    }
                                    return (double) (totalCount - matchingDocCount) / totalCount;
                                }
                            });
                        }
                    });
            log.debug("cacheKey:{} msg:FILTER_ESTIMATION_COMPLETED field:{} fieldMultiplier:{} overallOldMultiplier:{} overallNewMultiplier:{}",
                    cacheKey, filterField, currentFilterMultiplier, overallFilterMultiplier, overallFilterMultiplier * currentFilterMultiplier);
            overallFilterMultiplier *= currentFilterMultiplier;
        }
        return (long) (currentDocCount * overallFilterMultiplier);
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
