package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Period;
import com.google.common.collect.Maps;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;

import java.util.Map;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
public class Utils {

    public static AbstractAggregationBuilder buildExtendedStatsAggregation(String field) {
        String metricKey = getExtendedStatsAggregationKey(field);
        return AggregationBuilders.extendedStats(metricKey).field(field);
    }

    public static AbstractAggregationBuilder buildPercentileAggregation(String field) {
        String metricKey = getPercentileAggregationKey(field);
        return AggregationBuilders.percentiles(metricKey).field(field);
    }

    public static DateHistogramBuilder buildDateHistogramAggregation(String field, DateHistogram.Interval interval) {
        String metricKey = getDateHistogramKey(field);
        return AggregationBuilders.dateHistogram(metricKey)
                .minDocCount(1)
                .field(field)
                .interval(interval);
    }

    public static DateHistogram.Interval getHistogramInterval(Period period) {
        DateHistogram.Interval interval;
        switch (period) {
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
        return interval;
    }

    public static String sanitizeFieldForAggregation(String field) {
        return field.replaceAll(Constants.FIELD_REPLACEMENT_REGEX, Constants.FIELD_REPLACEMENT_VALUE);
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

    public static Map<String, Number> createExtendedStatsResponse(InternalExtendedStats extendedStats) {
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

    public static Map<Number, Number> createPercentilesResponse(InternalPercentiles internalPercentiles) {
        Map<Number, Number> percentiles = Maps.newHashMap();
        for (Percentile percentile : internalPercentiles) {
            percentiles.put(percentile.getPercent(), percentile.getValue());
        }
        return percentiles;
    }

}
