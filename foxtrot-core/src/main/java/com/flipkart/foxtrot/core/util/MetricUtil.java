package com.flipkart.foxtrot.core.util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.concurrent.TimeUnit;

/**
 * Created by rishabh.goyal on 05/09/15.
 */
public class MetricUtil {

    private static final MetricUtil metricsHelper;
    private static final String PACKAGE_PREFIX = "com.flipkart.foxtrot.core";
    private static final String ACTION_METRIC_PREFIX = "action";
    private static final String CARDINALITY_METRIC_PREFIX = "cardinality";
    private static final String FILTER_METRIC_PREFIX = "filter";
    private static final String DOT_CONCATENATED_FOUR_VARIABLES = "%s.%s.%s.%s";
    private static final String DOT_CONCATENATED_THREE_VARIABLES = "%s.%s.%s";

    private static MetricRegistry metrics;

    static {
        metrics = new MetricRegistry();
        metricsHelper = new MetricUtil();
    }

    private MetricUtil() {
        JmxReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.MINUTES)
                .build()
                .start();
    }

    public static void setup(MetricRegistry metrics) {
        MetricUtil.metrics = metrics;
    }

    public static MetricUtil getInstance() {
        return metricsHelper;
    }

    public void registerActionCacheHit(String opCode) {
        registerActionCacheOperation(opCode, "success");
    }

    private void registerActionCacheOperation(String opCode,
                                              String status) {
        metrics.meter(String.format("%s.%s.cache.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, status))
                .mark();
        metrics.meter(String.format("%s.%s.%s.cache.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opCode, status))
                .mark();
    }

    public void registerActionCacheMiss(String opCode) {
        registerActionCacheOperation(opCode, "failure");
    }

    public void registerActionSuccess(String metricKey,
                                      long duration) {
        registerActionOperation(metricKey, "success", duration);
    }

    private void registerActionOperation(String metricKey,
                                         String status,
                                         long duration) {
        metrics.timer(String.format(DOT_CONCATENATED_THREE_VARIABLES, PACKAGE_PREFIX, ACTION_METRIC_PREFIX, metricKey))
                .update(duration, TimeUnit.MILLISECONDS);
        metrics.timer(
                String.format(DOT_CONCATENATED_FOUR_VARIABLES, PACKAGE_PREFIX, ACTION_METRIC_PREFIX, metricKey, status))
                .update(duration, TimeUnit.MILLISECONDS);
    }

    public void registerActionFailure(String metricKey,
                                      long duration) {
        registerActionOperation(metricKey, "failure", duration);
    }

    public void registerCardinalityValidationOperation(String opCode,
                                                       String status) {
        metrics.meter(
                String.format(DOT_CONCATENATED_THREE_VARIABLES, PACKAGE_PREFIX, CARDINALITY_METRIC_PREFIX, status))
                .mark();
        metrics.meter(String.format(DOT_CONCATENATED_FOUR_VARIABLES, PACKAGE_PREFIX, CARDINALITY_METRIC_PREFIX, opCode,
                status))
                .mark();
    }

    public void registerFilterUsage(String filterOperator) {
        metrics.meter(
                String.format(DOT_CONCATENATED_THREE_VARIABLES, PACKAGE_PREFIX, FILTER_METRIC_PREFIX, filterOperator))
                .mark();
    }

    public void registerFilterUsage(String table,
                                    String filterOperator) {
        metrics.meter(String.format(DOT_CONCATENATED_FOUR_VARIABLES, PACKAGE_PREFIX, FILTER_METRIC_PREFIX, table,
                filterOperator))
                .mark();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

}
