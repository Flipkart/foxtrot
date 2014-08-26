package com.flipkart.foxtrot.core.querystore.actions;

import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
public class StatsUtils {

    public static String getDateHistogramKey(){
        return ActionConstants.TIMESTAMP_FIELD.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE);
    }

    public static AbstractAggregationBuilder buildSingleStatsAggregation(String field) {
        String metricKey = getExtendedStatsAggregationKey(field);
        return AggregationBuilders.extendedStats(metricKey).field(field);
    }

    public static AbstractAggregationBuilder buildPercentileAggregation(String field) {
        String metricKey = getPercentileAggregationKey(field);
        return AggregationBuilders.percentiles(metricKey).field(field);
    }

    public static String getExtendedStatsAggregationKey(String field){
        return field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE) + "_extended_stats";
    }

    public static String getPercentileAggregationKey(String field){
        return field.replaceAll(ActionConstants.AGGREGATION_FIELD_REPLACEMENT_REGEX, ActionConstants.AGGREGATION_FIELD_REPLACEMENT_VALUE) + "_percentile";
    }
}
