package com.flipkart.foxtrot.core.querystore.actions;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;

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

    public static AbstractAggregationBuilder buildDateHistogramAggregation(String field, DateHistogram.Interval interval) {
        String metricKey = getDateHistogramKey(field);
        return AggregationBuilders.dateHistogram(metricKey)
                .field(field)
                .interval(interval);
    }

    public static String sanitizeFieldForAggregation(String field){
        return field.replaceAll(Constants.FIELD_REPLACEMENT_REGEX, Constants.FIELD_REPLACEMENT_VALUE);
    }

    public static String getExtendedStatsAggregationKey(String field){
        return sanitizeFieldForAggregation(field) + "_extended_stats";
    }

    public static String getPercentileAggregationKey(String field){
        return sanitizeFieldForAggregation(field) + "_percentile";
    }

    public static String getDateHistogramKey(String field){
        return sanitizeFieldForAggregation(field) + "_date_histogram";
    }
    
    public static IndicesOptions indicesOptions() {
        return IndicesOptions.fromOptions(true, true, true, false);
    }
}
