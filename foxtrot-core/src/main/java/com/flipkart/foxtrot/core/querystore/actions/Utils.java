package com.flipkart.foxtrot.core.querystore.actions;


import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.val;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.joda.time.DateTimeZone;

import java.util.*;

import static com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils.QUERY_SIZE;
import java.util.Objects;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
public class Utils {

    private static final double[] DEFAULT_PERCENTILES = {1d, 5d, 25, 50d, 75d, 95d, 99d};

    public static TermsAggregationBuilder buildTermsAggregation(List<ResultSort> fields, Set<AggregationBuilder> subAggregations) {
        TermsAggregationBuilder rootBuilder = null;
        TermsAggregationBuilder termsBuilder = null;
        for(ResultSort nestingField : fields) {
            String field = nestingField.getField();
            BucketOrder bucketOrder = BucketOrder.key(nestingField.getOrder() != ResultSort.Order.desc);
            if(null == termsBuilder) {
                termsBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field))
                        .field(storedFieldName(field))
                        .order(bucketOrder);
            } else {
                TermsAggregationBuilder tempBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field))
                        .field(storedFieldName(field))
                        .order(bucketOrder);
                termsBuilder.subAggregation(tempBuilder);
                termsBuilder = tempBuilder;
            }
            termsBuilder.size(QUERY_SIZE);
            if(null == rootBuilder) {
                rootBuilder = termsBuilder;
            }
        }
        if(!CollectionUtils.isNullOrEmpty(subAggregations)) {
            assert termsBuilder != null;
            for(AggregationBuilder aggregationBuilder : subAggregations) {
                termsBuilder.subAggregation(aggregationBuilder);
            }
        }
        return rootBuilder;
    }

    public static AbstractAggregationBuilder buildStatsAggregation(String field, Set<Stat> stats) {
        String metricKey = getExtendedStatsAggregationKey(field);

        boolean anyExtendedStat = stats == null || stats.stream()
                .anyMatch(Stat::isExtended);
        if(anyExtendedStat) {
            return AggregationBuilders.extendedStats(metricKey)
                    .field(storedFieldName(field));
        }

        if(stats.size() > 1) {
            return AggregationBuilders.stats(metricKey)
                    .field(storedFieldName(field));
        }
        val stat = stats.iterator()
                .next();

        return stat.visit(new Stat.StatVisitor<AbstractAggregationBuilder>() {
            @Override
            public AbstractAggregationBuilder visitCount() {
                return AggregationBuilders.count(metricKey)
                        .field(storedFieldName(field));
            }

            @Override
            public AbstractAggregationBuilder visitMin() {
                return AggregationBuilders.min(metricKey)
                        .field(field);
            }

            @Override
            public AbstractAggregationBuilder visitMax() {
                return AggregationBuilders.max(metricKey)
                        .field(storedFieldName(field));
            }

            @Override
            public AbstractAggregationBuilder visitAvg() {
                return AggregationBuilders.avg(metricKey)
                        .field(storedFieldName(field));
            }

            @Override
            public AbstractAggregationBuilder visitSum() {
                return AggregationBuilders.sum(metricKey)
                        .field(storedFieldName(field));
            }

            @Override
            public AbstractAggregationBuilder visitSumOfSquares() {
                throw FoxtrotExceptions.createServerException("InvalidCodePathForSumOfSquares", null);
            }

            @Override
            public AbstractAggregationBuilder visitVariance() {
                throw FoxtrotExceptions.createServerException("InvalidCodePathForVariance", null);
            }

            @Override
            public AbstractAggregationBuilder visitStdDeviation() {
                throw FoxtrotExceptions.createServerException("InvalidCodePathForStdDeviation", null);
            }
        });
    }

    public static AbstractAggregationBuilder buildPercentileAggregation(String field, Collection<Double> inputPercentiles) {
        double[] percentiles = inputPercentiles != null ? inputPercentiles.stream()
                .mapToDouble(x -> x)
                .toArray() : DEFAULT_PERCENTILES;
        String metricKey = getPercentileAggregationKey(field);
        return AggregationBuilders.percentiles(metricKey)
                .percentiles(percentiles)
                .field(storedFieldName(field));
    }

    public static DateHistogramAggregationBuilder buildDateHistogramAggregation(String field, DateHistogramInterval interval) {
        String metricKey = getDateHistogramKey(field);
        return AggregationBuilders.dateHistogram(metricKey)
                .minDocCount(0)
                .field(storedFieldName(field))
                .timeZone(DateTimeZone.getDefault())
                .dateHistogramInterval(interval);
    }

    public static CardinalityAggregationBuilder buildCardinalityAggregation(String field) {
        return AggregationBuilders.cardinality(Utils.sanitizeFieldForAggregation(field))
                .precisionThreshold(500)
                .field(storedFieldName(field));
    }

    public static String sanitizeFieldForAggregation(String field) {
        return field.replaceAll(Constants.FIELD_REPLACEMENT_REGEX, Constants.FIELD_REPLACEMENT_VALUE);
    }

    public static String storedFieldName(String field) {
        if("_timestamp".equalsIgnoreCase(field)) {
            return ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME;
        }
        return field;
    }

    public static DateHistogramInterval getHistogramInterval(Period period) {
        DateHistogramInterval interval;
        switch (period) {
            case seconds:
                interval = DateHistogramInterval.SECOND;
                break;
            case minutes:
                interval = DateHistogramInterval.MINUTE;
                break;
            case hours:
                interval = DateHistogramInterval.HOUR;
                break;
            case days:
                interval = DateHistogramInterval.DAY;
                break;
            default:
                interval = DateHistogramInterval.HOUR;
                break;
        }
        return interval;
    }

    public static String getExtendedStatsAggregationKey(String field) {
        return sanitizeFieldForAggregation(field) + "_extended_stats";
    }

    public static String getPercentileAggregationKey(String field) {
        return sanitizeFieldForAggregation(field) + "_percentile";
    }

    public static String getDateHistogramKey(String field) {
        return sanitizeFieldForAggregation(field) + "_date_histogram";
    }

    public static IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    public static Map<String, Number> createStatsResponse(InternalExtendedStats extendedStats) {
        Map<String, Number> stats = Maps.newHashMap();
        stats.put("avg", extendedStats.getAvg());
        stats.put("sum", extendedStats.getSum());
        stats.put("count", extendedStats.getCount());
        stats.put("min", extendedStats.getMin());
        stats.put("max", extendedStats.getMax());
        stats.put("sum_of_squares", extendedStats.getSumOfSquares());
        stats.put("variance", extendedStats.getVariance());
        stats.put("std_deviation", extendedStats.getStdDeviation());
        return stats;
    }

    public static Map<String, Number> createStatsResponse(InternalStats internalStats) {
        Map<String, Number> stats = Maps.newHashMap();
        stats.put("avg", internalStats.getAvg());
        stats.put("sum", internalStats.getSum());
        stats.put("count", internalStats.getCount());
        stats.put("min", internalStats.getMin());
        stats.put("max", internalStats.getMax());
        return stats;
    }

    public static Map<String, Number> createStatResponse(InternalMax statAggregation) {
        return ImmutableMap.of("max", statAggregation.getValue());
    }

    public static Map<String, Number> createStatResponse(InternalMin statAggregation) {
        return ImmutableMap.of("min", statAggregation.getValue());
    }

    public static Map<String, Number> createStatResponse(InternalAvg statAggregation) {
        return ImmutableMap.of("avg", statAggregation.getValue());
    }

    public static Map<String, Number> createStatResponse(InternalSum statAggregation) {
        return ImmutableMap.of("sum", statAggregation.getValue());
    }

    public static Map<String, Number> createStatResponse(InternalValueCount statAggregation) {
        return ImmutableMap.of("count", statAggregation.getValue());
    }

    public static Map<Number, Number> createPercentilesResponse(Percentiles internalPercentiles) {
        Map<Number, Number> percentiles = Maps.newHashMap();
        for(Percentile percentile : internalPercentiles) {
            percentiles.put(percentile.getPercent(), percentile.getValue());
        }
        return percentiles;
    }

    public static double ensurePositive(long number) {
        return number <= 0.0 ? 0.0 : number;
    }


    public static double ensureOne(long number) {
        return number <= 0 ? 1 : number;
    }

    public static Map<String, Number> toStats(Aggregation statAggregation) {
        if(statAggregation instanceof InternalExtendedStats) {
            return Utils.createStatsResponse((InternalExtendedStats)statAggregation);
        } else if(statAggregation instanceof InternalStats) {
            return Utils.createStatsResponse((InternalStats)statAggregation);
        } else if(statAggregation instanceof InternalMax) {
            return Utils.createStatResponse((InternalMax)statAggregation);
        } else if(statAggregation instanceof InternalMin) {
            return Utils.createStatResponse((InternalMin)statAggregation);
        } else if(statAggregation instanceof InternalAvg) {
            return Utils.createStatResponse((InternalAvg)statAggregation);
        } else if(statAggregation instanceof InternalSum) {
            return Utils.createStatResponse((InternalSum)statAggregation);
        } else if(statAggregation instanceof InternalValueCount) {
            return Utils.createStatResponse((InternalValueCount)statAggregation);
        }
        return new HashMap<>();
    }

}
