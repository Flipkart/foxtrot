package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.Maps;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rishabh.goyal on 24/08/14.
 */
public class Utils {


    public static TermsAggregationBuilder buildTermsAggregation(List<ResultSort> fields,
                                                                Set<AggregationBuilder> subAggregations) {
        TermsAggregationBuilder rootBuilder = null;
        TermsAggregationBuilder termsBuilder = null;
        for (ResultSort nestingField : fields) {
            Terms.Order order = (nestingField.getOrder() == ResultSort.Order.desc) ? Terms.Order.term(false) : Terms.Order.term(true);
            String field = nestingField.getField();
            if (null == termsBuilder) {
                termsBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field))
                        .field(storedFieldName(field))
                        .order(order);
            } else {
                TermsAggregationBuilder tempBuilder = AggregationBuilders.terms(Utils.sanitizeFieldForAggregation(field))
                        .field(storedFieldName(field))
                        .order(order);
                termsBuilder.subAggregation(tempBuilder);
                termsBuilder = tempBuilder;
            }
            termsBuilder.size(1000);
            if (null == rootBuilder) {
                rootBuilder = termsBuilder;
            }
        }
        if (subAggregations != null && !subAggregations.isEmpty()) {
            assert termsBuilder != null;
            for (AggregationBuilder aggregationBuilder : subAggregations) {
                termsBuilder.subAggregation(aggregationBuilder);
            }
        }
        return rootBuilder;
    }

    public static AbstractAggregationBuilder buildExtendedStatsAggregation(String field) {
        String metricKey = getExtendedStatsAggregationKey(field);
        return AggregationBuilders.extendedStats(metricKey).field(storedFieldName(field));
    }

    public static AbstractAggregationBuilder buildPercentileAggregation(String field) {
        String metricKey = getPercentileAggregationKey(field);
        return AggregationBuilders.percentiles(metricKey).field(storedFieldName(field));
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
        return AggregationBuilders
                .cardinality(Utils.sanitizeFieldForAggregation(field))
                .precisionThreshold(40000)
                .field(storedFieldName(field));
    }

    public static String sanitizeFieldForAggregation(String field) {
        return field.replaceAll(Constants.FIELD_REPLACEMENT_REGEX, Constants.FIELD_REPLACEMENT_VALUE);
    }

    public static String storedFieldName(String field) {
        if ("_timestamp".equalsIgnoreCase(field)) {
            return ElasticsearchUtils.DOCUMENT_TIMESTAMP_FIELD_NAME;
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

    public static Map<Number, Number> createPercentilesResponse(Percentiles internalPercentiles) {
        Map<Number, Number> percentiles = Maps.newHashMap();
        for (Percentile percentile : internalPercentiles) {
            percentiles.put(percentile.getPercent(), percentile.getValue());
        }
        return percentiles;
    }

}
