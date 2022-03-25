package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.cardinality.ProbabilityCalculationResult;
import com.flipkart.foxtrot.common.cardinality.ProbabilityCalculationResult.ProbabilityCalculationResultBuilder;
import com.flipkart.foxtrot.common.estimation.*;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitorAdapter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotInFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Interval;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Singleton
public class CardinalityValidatorImpl implements CardinalityValidator {

    private static final long MAX_CARDINALITY = 50000;
    private static final long MIN_ESTIMATION_THRESHOLD = 1000;
    private static final double PROBABILITY_CUT_OFF = 0.5;
    private static final String BLOCK_ACTION = "block";
    private static final String ALLOW_ACTION = "allow";
    private final QueryStore queryStore;
    private final TableMetadataManager tableMetadataManager;


    @Inject
    public CardinalityValidatorImpl(final QueryStore queryStore,
                                    final TableMetadataManager tableMetadataManager) {
        this.queryStore = queryStore;
        this.tableMetadataManager = tableMetadataManager;
    }


    @Override
    public void validateCardinality(Action action,
                                    ActionRequest actionRequest,
                                    String table,
                                    List<String> groupingColumns) {
        if (CollectionUtils.isNullOrEmpty(groupingColumns)) {
            return;
        }
        final String requestString = action.requestString();
        final String cacheKey = action.getRequestCacheKey();

        // Perform cardinality analysis and see how much this fucks up the cluster
        if (queryStore instanceof ElasticsearchQueryStore
                && ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                .isEnabled()) {
            ProbabilityCalculationResult probabilityResult = ProbabilityCalculationResult.builder()
                    .probability(0.0)
                    .build();
            try {
                TableFieldMapping fieldMappings = tableMetadataManager.getFieldMappingsWithCardinality(table);
                if (null == fieldMappings) {
                    fieldMappings = TableFieldMapping.builder()
                            .mappings(Collections.emptySet())
                            .table(table)
                            .build();
                }

                if (log.isDebugEnabled()) {
                    log.debug("Estimating probability with cardinality estimation for cacheKey:{} , request:{}",
                            cacheKey, requestString);
                }
                probabilityResult = estimateProbability(fieldMappings, actionRequest, cacheKey, table, groupingColumns);
            } catch (Exception e) {
                log.error("Error running cardinality estimation for request:{} , cacheKey:{} ", requestString, cacheKey,
                        e);
            }

            if (probabilityResult.getProbability() > PROBABILITY_CUT_OFF) {
                log.info(
                        "Blocked query as it might have screwed up the cluster. Probability: {} Query: {}, cacheKey: {}",
                        probabilityResult, requestString, cacheKey);
                MetricUtil.getInstance()
                        .registerCardinalityValidationOperation(actionRequest.getOpcode(), BLOCK_ACTION);
                throw FoxtrotExceptions.createCardinalityOverflow(actionRequest, requestString, cacheKey,
                        groupingColumns, probabilityResult);
            } else {
                log.info("Allowing group by with probability: {} for query: {}, cacheKey: {}", probabilityResult,
                        requestString, cacheKey);
                MetricUtil.getInstance()
                        .registerCardinalityValidationOperation(actionRequest.getOpcode(), ALLOW_ACTION);
            }
        }
    }

    private ProbabilityCalculationResult estimateProbability(TableFieldMapping tableFieldMapping,
                                                             ActionRequest actionRequest,
                                                             String cacheKey,
                                                             String table,
                                                             List<String> groupingColumns) {

        Set<FieldMetadata> mappings = tableFieldMapping.getMappings();
        Map<String, FieldMetadata> metaMap = mappings.stream()
                .collect(Collectors.toMap(FieldMetadata::getField, mapping -> mapping));

        try {
            long maxCardinality = getMaxCardinality();

            long estimatedMaxDocCount = extractMaxDocCount(metaMap);
            if (log.isDebugEnabled()) {
                log.debug("cacheKey:{} msg:DOC_COUNT_ESTIMATION_COMPLETED maxDocCount:{}", cacheKey,
                        estimatedMaxDocCount);
            }
            long estimatedDocCountBasedOnTime = estimateDocCountBasedOnTime(estimatedMaxDocCount, actionRequest,
                    tableMetadataManager, tableFieldMapping.getTable());
            if (log.isDebugEnabled()) {
                log.debug(
                        "cacheKey:{} msg:TIME_BASED_DOC_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{}",
                        cacheKey, estimatedMaxDocCount, estimatedDocCountBasedOnTime);
            }
            long estimatedDocCountAfterFilters = estimateDocCountWithFilters(estimatedDocCountBasedOnTime, metaMap,
                    actionRequest.getFilters(), cacheKey);
            if (log.isDebugEnabled()) {
                log.debug("cacheKey:{} msg:ALL_FILTER_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{} "
                                + "docCountAfterFilters:{}", cacheKey, estimatedMaxDocCount, estimatedDocCountBasedOnTime,
                        estimatedDocCountAfterFilters);
            }
            if (estimatedDocCountAfterFilters < MIN_ESTIMATION_THRESHOLD) {
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} msg:NESTING_ESTIMATION_SKIPPED estimatedDocCount:{} threshold:{}", cacheKey,
                            estimatedDocCountAfterFilters, MIN_ESTIMATION_THRESHOLD);
                }
                return ProbabilityCalculationResult.builder()
                        .probability(0.0)
                        .build();
            }

            long outputCardinality = 1;
            final AtomicBoolean reduceCardinality = new AtomicBoolean(false);
            Map<String, Long> groupingColumnCardinality = new HashMap<>();

            for (final String field : groupingColumns) {
                FieldMetadata metadata = metaMap.get(field);
                if (null == metadata || null == metadata.getEstimationData()) {
                    log.warn("cacheKey:{} msg:NO_FIELD_ESTIMATION_DATA table:{} field:{}", cacheKey, table, field);
                    continue;
                }
                long fieldCardinality = metadata.getEstimationData()
                        .accept(new EstimationDataVisitor<Long>() {
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
                                return (long) (
                                        ((double) LongMath.checkedMultiply(cardinalityEstimationData.getCardinality(),
                                                estimatedDocCountAfterFilters)) / cardinalityEstimationData.getCount());
                            }

                            @Override
                            public Long visit(TermHistogramEstimationData termEstimationData) {
                                reduceCardinality.getAndSet(true);
                                return (long) termEstimationData.getTermCounts()
                                        .size();
                            }
                        });
                fieldCardinality = (long) Utils.ensureOne(fieldCardinality);
                long newCardinality = LongMath.checkedMultiply(outputCardinality, fieldCardinality);
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} msg:NESTING_FIELD_ESTIMATION_COMPLETED field:{} overallCardinality:{} "
                                    + "fieldCardinality:{} newCardinality:{}", cacheKey, field, outputCardinality,
                            fieldCardinality, newCardinality);
                }
                groupingColumnCardinality.put(field, fieldCardinality);
                outputCardinality = newCardinality;
            }

            //Although cardinality will not be reduced by the same factor as documents count reduced.
            //To give benefit of doubt or if someone is making query on a smaller time frame using fields of higher
            // cardinality, reducing cardinality for that query
            //Only reducing cardinality if the doc count is actually less than docCount for a day. Assuming cardinality
            // will remain same if query for more than 1 day
            if (estimatedMaxDocCount != 0 && ((double) estimatedDocCountAfterFilters / estimatedMaxDocCount) < 1.0
                    && reduceCardinality.get()) {
                outputCardinality = (long) (
                        ((double) LongMath.checkedMultiply(outputCardinality, estimatedDocCountAfterFilters))
                                / estimatedMaxDocCount);
            }
            if (log.isDebugEnabled()) {
                log.debug(
                        "cacheKey:{} msg:NESTING_FIELDS_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{} "
                                + "docCountAfterFilters:{} outputCardinality:{}", cacheKey, estimatedMaxDocCount,
                        estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters, outputCardinality);
            }

            ProbabilityCalculationResultBuilder probabilityResultBuilder = ProbabilityCalculationResult.builder()
                    .estimatedMaxDocCount(estimatedMaxDocCount)
                    .estimatedDocCountBasedOnTime(estimatedDocCountBasedOnTime)
                    .estimatedDocCountAfterFilters(estimatedDocCountAfterFilters)
                    .groupingColumnCardinality(groupingColumnCardinality)
                    .outputCardinality(outputCardinality)
                    .maxCardinality(maxCardinality)
                    .cardinalityReduced(reduceCardinality.get());

            log.warn("Output cardinality : {}, estimatedMaxDocCount : {}, estimatedDocCountBasedOnTime : {}, "
                            + "estimatedDocCountAfterFilters : {},  Query: {}, cacheKey: {}", outputCardinality,
                    estimatedMaxDocCount, estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters,
                    JsonUtils.toJson(actionRequest), cacheKey);

            if (outputCardinality > maxCardinality) {
                return probabilityResultBuilder.probability(1.0)
                        .build();
            } else {
                return probabilityResultBuilder.probability(0.0)
                        .build();
            }
        } catch (ArithmeticException ae) {
            log.error("Arithmetic exception occured while cardinality calculation for cache Key :{} , "
                    + "probability of screwing up cluster is 100%", cacheKey, ae);
            return ProbabilityCalculationResult.builder()
                    .probability(1.0)
                    .build();
        }

    }

    private long getMaxCardinality() {
        long maxCardinality = MAX_CARDINALITY;
        if (queryStore instanceof ElasticsearchQueryStore
                && ((ElasticsearchQueryStore) queryStore).getCardinalityConfig() != null &&
                ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                        .getMaxCardinality() != 0) {
            maxCardinality = ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                    .getMaxCardinality();
        }
        return maxCardinality;
    }


    private long extractMaxDocCount(Map<String, FieldMetadata> metaMap) {
        return metaMap.values()
                .stream()
                .map(x -> x.getEstimationData() == null
                        ? 0
                        : x.getEstimationData()
                        .getCount())
                .max(Comparator.naturalOrder())
                .orElse((long) 0);
    }

    private long estimateDocCountBasedOnTime(long currentDocCount,
                                             ActionRequest actionRequest,
                                             TableMetadataManager tableMetadataManager,
                                             String table) {
        Interval queryInterval = new PeriodSelector(actionRequest.getFilters()).analyze();
        long minutes = queryInterval.toDuration()
                .getStandardMinutes();
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
            return LongMath.checkedMultiply(currentDocCount, maxDays);
        } else {
            return LongMath.checkedMultiply(currentDocCount, (long) days);
        }
    }

    private long estimateDocCountWithFilters(long currentDocCount,
                                             Map<String, FieldMetadata> metaMap,
                                             List<Filter> filters,
                                             String cacheKey) {
        if (CollectionUtils.isNullOrEmpty(filters)) {
            return currentDocCount;
        }

        double overallFilterMultiplier = 1;
        for (Filter filter : filters) {
            final String filterField = filter.getField();
            FieldMetadata fieldMetadata = metaMap.get(filterField);
            if (null == fieldMetadata || null == fieldMetadata.getEstimationData()) {
                log.warn("cacheKey:{} msg:NO_FIELD_ESTIMATION_DATA field:{}", cacheKey, filterField);
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("cacheKey:{} msg:FILTER_ESTIMATION_STARTED filter:{} mapping:{}", cacheKey, filter,
                        fieldMetadata);
            }
            double currentFilterMultiplier = fieldMetadata.getEstimationData()
                    .accept(getDocCountWithFilterEstimationDataVisitor(filter, cacheKey));
            if (log.isDebugEnabled()) {
                log.debug(
                        "cacheKey:{} msg:FILTER_ESTIMATION_COMPLETED field:{} fieldMultiplier:{} overallOldMultiplier:{} "
                                + "overallNewMultiplier:{}", cacheKey, filterField, currentFilterMultiplier,
                        overallFilterMultiplier, overallFilterMultiplier * currentFilterMultiplier);
            }
            overallFilterMultiplier *= currentFilterMultiplier;
        }
        return (long) (currentDocCount * overallFilterMultiplier);
    }

    private EstimationDataVisitor<Double> getDocCountWithFilterEstimationDataVisitor(Filter filter,
                                                                                     String cacheKey) {
        return new EstimationDataVisitor<Double>() {
            @Override
            @SneakyThrows
            public Double visit(FixedEstimationData fixedEstimationData) {
                return filter.accept(getFixedFilterVisitorAdapter(fixedEstimationData));
            }

            @Override
            @SneakyThrows
            public Double visit(PercentileEstimationData percentileEstimationData) {
                final double[] percentiles = percentileEstimationData.getValues();
                final long numMatches = percentileEstimationData.getCount();
                return filter.accept(getPercentileFilterVisitorAdapter(percentiles, cacheKey, numMatches));
            }

            @Override
            @SneakyThrows
            public Double visit(CardinalityEstimationData cardinalityEstimationData) {
                return filter.accept(getCardinalityFilterVisitorAdapter(cardinalityEstimationData));
            }

            @Override
            @SneakyThrows
            public Double visit(TermHistogramEstimationData termEstimationData) {
                long totalCount = termEstimationData.getCount();
                return filter.accept(getTermHistogramFilterVisitorAdapter(termEstimationData, totalCount));
            }
        };
    }

    private FilterVisitorAdapter<Double> getFixedFilterVisitorAdapter(FixedEstimationData fixedEstimationData) {
        return new FilterVisitorAdapter<Double>(1.0) {

            @Override
            public Double visit(EqualsFilter equalsFilter) {
                //If there is a match it will be atmost one out of all the values present
                return 1.0 / Utils.ensureOne(fixedEstimationData.getCount());
            }

            @Override
            public Double visit(NotEqualsFilter notEqualsFilter) {
                // Assuming a match, there will be N-1 unmatched values
                double numerator = Utils.ensurePositive(fixedEstimationData.getCount() - 1);
                return numerator / Utils.ensureOne(fixedEstimationData.getCount());

            }

            @Override
            public Double visit(ContainsFilter stringContainsFilterElement) {
                // Assuming there is a match to a value.
                // Can be more, but we err on the side of optimism.
                return (1.0 / Utils.ensureOne(fixedEstimationData.getCount()));

            }

            @Override
            public Double visit(InFilter inFilter) {
                // Assuming there are M matches, the probability is M/N
                return Utils.ensurePositive(inFilter.getValues()
                        .size()) / Utils.ensureOne(fixedEstimationData.getCount());
            }

            @Override
            public Double visit(NotInFilter notInFilter) {
                // Assuming there are M matches, then probability will be N - M / N
                return Utils.ensurePositive(fixedEstimationData.getCount() - notInFilter.getValues()
                        .size()) / Utils.ensureOne(fixedEstimationData.getCount());
            }
        };
    }

    private FilterVisitorAdapter<Double> getPercentileFilterVisitorAdapter(double[] percentiles,
                                                                           String cacheKey,
                                                                           long numMatches) {
        return new FilterVisitorAdapter<Double>(1.0) {
            @Override
            public Double visit(BetweenFilter betweenFilter) {

                //What percentage percentiles are >= above lower bound
                int minBound = IntStream.rangeClosed(0, 9)
                        .filter(i -> betweenFilter.getFrom()
                                .doubleValue() <= percentiles[i])
                        .findFirst()
                        .orElse(0);
                // What percentage of values are > upper bound
                int maxBound = IntStream.rangeClosed(0, 9)
                        .filter(i -> betweenFilter.getTo()
                                .doubleValue() < percentiles[i])
                        .findFirst()
                        .orElse(9);

                int numBuckets = maxBound - minBound + 1;
                final double result = (double) numBuckets / 10.0;
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} Between filter: {} " + "percentiles[{}] = {} to percentiles[{}] = {} "
                                    + "buckets {} multiplier {}", cacheKey, betweenFilter, minBound, percentiles[minBound],
                            maxBound, percentiles[maxBound], numBuckets, result);
                }
                return result;
            }

            @Override
            public Double visit(EqualsFilter equalsFilter) {
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
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} EqualsFilter:{} numMatches:{} multiplier:{}", cacheKey, equalsFilter,
                            numMatches, result);
                }
                return result;
            }

            @Override
            public Double visit(NotEqualsFilter notEqualsFilter) {
                // There is no match, so all values will be considered
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} NotEqualsFilter:{} multiplier: 1.0", cacheKey, notEqualsFilter);
                }
                return 1.0;
            }

            @Override
            public Double visit(GreaterThanFilter greaterThanFilter) {
                //Percentage of values greater than given value
                //Found when we find a percentile value > bound
                int minBound = IntStream.rangeClosed(0, 9)
                        .filter(i -> percentiles[i] > greaterThanFilter.getValue()
                                .doubleValue())
                        //Stop when we find a value
                        .findFirst()
                        .orElse(0);

                //Everything below this percentile do not affect
                final double result = (double) (10 - minBound - 1) / 10.0;
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} GreaterThanFilter: {} percentiles[{}] = {} multiplier: {}", cacheKey,
                            greaterThanFilter, minBound, percentiles[minBound], result);
                }
                return result;
            }

            @Override
            public Double visit(GreaterEqualFilter greaterEqualFilter) {
                //Percentage of values greater than or equal to given value
                //Found when we find a percentile value > bound
                int minBound = IntStream.rangeClosed(0, 9)
                        .filter(i -> percentiles[i] >= greaterEqualFilter.getValue()
                                .doubleValue())
                        //Stop when we find a value >= bound
                        .findFirst()
                        .orElse(0);

                //Everything below this do not affect
                final double result = (double) (10 - minBound - 1) / 10.0;
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} GreaterEqualsFilter:{} percentiles[{}] = {} multiplier: {}", cacheKey,
                            greaterEqualFilter, minBound, percentiles[minBound], result);
                }
                return result;
            }

            @Override
            public Double visit(LessThanFilter lessThanFilter) {
                //Percentage of values lesser than to bound
                //Found when we find a percentile value >= bound
                int minBound = 9 - IntStream.rangeClosed(0, 9)
                        .filter(i -> percentiles[9 - i] < lessThanFilter.getValue()
                                .doubleValue())
                        //Stop when we find a value >= bound
                        .findFirst()
                        .orElse(9);

                //Everything above this do not affect
                final double result = ((double) minBound + 1.0) / 10.0;
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} LessThanFilter:{} percentiles[{}] = {} multiplier: {}", cacheKey,
                            lessThanFilter, minBound, percentiles[minBound], result);
                }
                return result;
            }

            @Override
            public Double visit(LessEqualFilter lessEqualFilter) {
                //Percentage of values lesser than or equal to bound
                //Found when we find a percentile value > bound
                int minBound = 9 - IntStream.rangeClosed(0, 9)
                        .filter(i -> percentiles[9 - i] <= lessEqualFilter.getValue()
                                .doubleValue())
                        //Stop when we find a value > bound
                        .findFirst()
                        .orElse(9);
                //Everything above this do not affect
                final double result = ((double) minBound + 1.0) / 10.0;
                if (log.isDebugEnabled()) {
                    log.debug("cacheKey:{} LessEqualsFilter: {} percentiles[{}] = {} multiplier: {}", cacheKey,
                            lessEqualFilter, minBound, percentiles[minBound], result);
                }
                return result;
            }
        };
    }

    private FilterVisitorAdapter<Double> getCardinalityFilterVisitorAdapter(CardinalityEstimationData cardinalityEstimationData) {
        return new FilterVisitorAdapter<Double>(1.0) {

            @Override
            public Double visit(EqualsFilter equalsFilter) {
                //If there is a match it will be atmost one out of all the values present
                return 1.0 / Utils.ensureOne(cardinalityEstimationData.getCardinality());
            }

            @Override
            public Double visit(NotEqualsFilter notEqualsFilter) {
                // Assuming a match, there will be N-1 unmatched values
                double numerator = Utils.ensurePositive(cardinalityEstimationData.getCardinality() - 1);
                return numerator / Utils.ensureOne(cardinalityEstimationData.getCardinality());

            }

            @Override
            public Double visit(ContainsFilter stringContainsFilterElement) {
                // Assuming there is a match to a value.
                // Can be more, but we err on the side of optimism.
                return (1.0 / Utils.ensureOne(cardinalityEstimationData.getCardinality()));

            }

            @Override
            public Double visit(InFilter inFilter) {
                // Assuming there are M matches, the probability is M/N
                return Utils.ensurePositive(inFilter.getValues()
                        .size()) / Utils.ensureOne(cardinalityEstimationData.getCardinality());
            }

            @Override
            public Double visit(NotInFilter notInFilter) {
                // Assuming there are M matches, then probability will be N - M / N
                return Utils.ensurePositive(cardinalityEstimationData.getCardinality() - notInFilter.getValues()
                        .size()) / Utils.ensureOne(cardinalityEstimationData.getCardinality());
            }
        };
    }

    private FilterVisitorAdapter<Double> getTermHistogramFilterVisitorAdapter(TermHistogramEstimationData termEstimationData,
                                                                              long totalCount) {
        return new FilterVisitorAdapter<Double>(1.0) {
            @Override
            public Double visit(EqualsFilter equalsFilter) {
                if (!(equalsFilter.getValue() instanceof String) || !termEstimationData.getTermCounts()
                        .containsKey(equalsFilter.getValue())) {
                    return 1.0;
                }
                long matchingDocCount = termEstimationData.getTermCounts()
                        .get(equalsFilter.getValue());
                return (double) matchingDocCount / totalCount;
            }

            @Override
            public Double visit(NotEqualsFilter notEqualsFilter) {
                if (!(notEqualsFilter.getValue() instanceof String) || !termEstimationData.getTermCounts()
                        .containsKey(notEqualsFilter.getValue())) {
                    return 1.0;
                }
                long matchingDocCount = termEstimationData.getTermCounts()
                        .get(notEqualsFilter.getValue());
                return (double) (totalCount - matchingDocCount) / totalCount;
            }

            @Override
            public Double visit(InFilter inFilter) {
                if (!isObjectInstanceOfString(inFilter.getValues())) {
                    return 1.0;
                }

                long matchingDocCount = 0;
                for (Object value : inFilter.getValues()) {
                    Long count = termEstimationData.getTermCounts()
                            .get(value);
                    matchingDocCount += getValidCount(count);
                }
                return (double) (matchingDocCount) / totalCount;
            }

            @Override
            public Double visit(NotInFilter notInFilter) {
                if (!isObjectInstanceOfString(notInFilter.getValues())) {
                    return 1.0;
                }

                long matchingDocCount = 0;
                for (Object value : notInFilter.getValues()) {
                    Long count = termEstimationData.getTermCounts()
                            .get(value);
                    matchingDocCount += getValidCount(count);
                }
                return (double) (totalCount - matchingDocCount) / totalCount;
            }
        };
    }

    private boolean isObjectInstanceOfString(List<Object> objects) {
        for (Object object : com.collections.CollectionUtils.nullSafeList(objects)) {
            if (!(object instanceof String)) {
                return false;
            }
        }
        return true;
    }

    private Long getValidCount(Long count) {
        return count == null
                ? 0
                : count;
    }
}
