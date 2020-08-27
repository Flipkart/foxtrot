package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.estimation.CardinalityEstimationData;
import com.flipkart.foxtrot.common.estimation.EstimationDataVisitor;
import com.flipkart.foxtrot.common.estimation.FixedEstimationData;
import com.flipkart.foxtrot.common.estimation.PercentileEstimationData;
import com.flipkart.foxtrot.common.estimation.TermHistogramEstimationData;
import com.flipkart.foxtrot.common.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.common.query.FilterVisitorAdapter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotInFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Interval;

@Slf4j
@Singleton
public class CardinalityValidatorImpl implements CardinalityValidator {

    private final QueryStore queryStore;
    private final TableMetadataManager tableMetadataManager;

    private static final long MAX_CARDINALITY = 50000;
    private static final long MIN_ESTIMATION_THRESHOLD = 1000;
    private static final double PROBABILITY_CUT_OFF = 0.5;

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

        // Perform cardinality analysis and see how much this fucks up the cluster
        if (queryStore instanceof ElasticsearchQueryStore
                && ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                .isEnabled()) {
            double probability = 0;
            try {
                TableFieldMapping fieldMappings = tableMetadataManager.getFieldMappings(table, true, false);
                if (null == fieldMappings) {
                    fieldMappings = TableFieldMapping.builder()
                            .mappings(Collections.emptySet())
                            .table(table)
                            .build();
                }

                String cacheKey = action.getRequestCacheKey();

                probability = estimateProbability(fieldMappings, actionRequest, cacheKey, table, groupingColumns);
            } catch (Exception e) {
                log.error("Error running estimation", e);
            }

            final String content = action.requestString();

            if (probability > PROBABILITY_CUT_OFF) {
                log.info("Blocked query as it might have screwed up the cluster. Probability: {} Query: {}",
                        probability, content);
                throw FoxtrotExceptions.createCardinalityOverflow(actionRequest, content, groupingColumns.get(0),
                        probability);
            } else {
                log.info("Allowing group by with probability {} for query: {}", probability, content);
            }
        }
    }

    private double estimateProbability(TableFieldMapping tableFieldMapping,
                                       ActionRequest actionRequest,
                                       String cacheKey,
                                       String table,
                                       List<String> groupingColumns) {
        Set<FieldMetadata> mappings = tableFieldMapping.getMappings();
        Map<String, FieldMetadata> metaMap = mappings.stream()
                .collect(Collectors.toMap(FieldMetadata::getField, mapping -> mapping));

        long estimatedMaxDocCount = extractMaxDocCount(metaMap);
        if (log.isDebugEnabled()) {
            log.debug("cacheKey:{} msg:DOC_COUNT_ESTIMATION_COMPLETED maxDocCount:{}", cacheKey, estimatedMaxDocCount);
        }
        long estimatedDocCountBasedOnTime = estimateDocCountBasedOnTime(estimatedMaxDocCount, actionRequest,
                tableMetadataManager, tableFieldMapping.getTable());
        if (log.isDebugEnabled()) {
            log.debug("cacheKey:{} msg:TIME_BASED_DOC_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{}",
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
            return 0.0;
        }

        long outputCardinality = 1;
        final AtomicBoolean reduceCardinality = new AtomicBoolean(false);
        for (int i = 0; i < groupingColumns.size(); i++) {
            final String field = groupingColumns.get(i);
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
                            return (long) (((double) (cardinalityEstimationData.getCardinality()
                                    * estimatedDocCountAfterFilters)) / cardinalityEstimationData.getCount());
                        }

                        @Override
                        public Long visit(TermHistogramEstimationData termEstimationData) {
                            reduceCardinality.getAndSet(true);
                            return (long) termEstimationData.getTermCounts()
                                    .size();
                        }
                    });
            if (log.isDebugEnabled()) {
                log.debug("cacheKey:{} msg:NESTING_FIELD_ESTIMATED field:{} overallCardinality:{} fieldCardinality:{} "
                                + "newCardinality:{}", cacheKey, field, outputCardinality, fieldCardinality,
                        outputCardinality * fieldCardinality);
            }
            fieldCardinality = (long) Utils.ensureOne(fieldCardinality);
            if (log.isDebugEnabled()) {
                log.debug("cacheKey:{} msg:NESTING_FIELD_ESTIMATION_COMPLETED field:{} overallCardinality:{} "
                                + "fieldCardinality:{} newCardinality:{}", cacheKey, field, outputCardinality, fieldCardinality,
                        outputCardinality * fieldCardinality);
            }
            outputCardinality *= fieldCardinality;
        }

        //Although cardinality will not be reduced by the same factor as documents count reduced.
        //To give benefit of doubt or if someone is making query on a smaller time frame using fields of higher
        // cardinality, reducing cardinality for that query
        //Only reducing cardinality if the doc count is actually less than docCount for a day. Assuming cardinality
        // will remain same if query for more than 1 day
        if (estimatedMaxDocCount != 0 && ((double) estimatedDocCountAfterFilters / estimatedMaxDocCount) < 1.0
                && reduceCardinality.get()) {
            outputCardinality = (long) (outputCardinality * ((double) estimatedDocCountAfterFilters
                    / estimatedMaxDocCount));
        }
        if (log.isDebugEnabled()) {
            log.debug("cacheKey:{} msg:NESTING_FIELDS_ESTIMATION_COMPLETED maxDocCount:{} docCountAfterTimeFilters:{} "
                            + "docCountAfterFilters:{} outputCardinality:{}", cacheKey, estimatedMaxDocCount,
                    estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters, outputCardinality);
        }
        long maxCardinality = MAX_CARDINALITY;
        if (queryStore instanceof ElasticsearchQueryStore
                && ((ElasticsearchQueryStore) queryStore).getCardinalityConfig() != null &&
                ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                        .getMaxCardinality() != 0) {
            maxCardinality = ((ElasticsearchQueryStore) queryStore).getCardinalityConfig()
                    .getMaxCardinality();
        }
        if (outputCardinality > maxCardinality) {
            log.warn("Output cardinality : {}, estimatedMaxDocCount : {}, estimatedDocCountBasedOnTime : {}, "
                            + "estimatedDocCountAfterFilters : {}, TableFieldMapping : {},  Query: {}", outputCardinality,
                    estimatedMaxDocCount, estimatedDocCountBasedOnTime, estimatedDocCountAfterFilters,
                    tableFieldMapping, actionRequest.toString());
            return 1.0;
        } else {
            return 0;
        }
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
            return currentDocCount * maxDays;
        } else {
            return (long) (currentDocCount * days);
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
