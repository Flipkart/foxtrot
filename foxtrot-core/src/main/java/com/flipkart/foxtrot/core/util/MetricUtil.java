package com.flipkart.foxtrot.core.util;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Created by rishabh.goyal on 05/09/15.
 */
public class MetricUtil {

    private static final MetricUtil metricsHelper;
    private static final String PACKAGE_PREFIX = "com.flipkart.foxtrot.core";
    private static final String ACTION_METRIC_PREFIX = "action";
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

    public void registerActionCacheHit(String opcode, String metricKey) {
        registerActionCacheOperation(opcode, metricKey, "success");
    }

    private void registerActionCacheOperation(String opcode, String metricKey, String status) {
        metrics.meter(String.format("%s.%s.cache.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, status))
                .mark();
        metrics.meter(String.format("%s.%s.%s.cache.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opcode, status))
                .mark();
        metrics.meter(
                String.format("%s.%s.%s.%s.cache.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opcode, metricKey, status))
                .mark();
    }

    public void registerActionCacheMiss(String opcode, String metricKey) {
        registerActionCacheOperation(opcode, metricKey, "failure");
    }

    public void registerActionSuccess(String opcode, String metricKey, long duration) {
        registerActionOperation(opcode, metricKey, "success", duration);
    }

    private void registerActionOperation(String opcode, String metricKey, String status, long duration) {
        metrics.timer(String.format("%s.%s.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opcode))
                .update(duration, TimeUnit.MILLISECONDS);
        metrics.timer(String.format("%s.%s.%s.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opcode, metricKey))
                .update(duration, TimeUnit.MILLISECONDS);
        metrics.timer(String.format("%s.%s.%s.%s.%s", PACKAGE_PREFIX, ACTION_METRIC_PREFIX, opcode, metricKey, status))
                .update(duration, TimeUnit.MILLISECONDS);
    }

    public void registerActionFailure(String opcode, String metricKey, long duration) {
        registerActionOperation(opcode, metricKey, "failure", duration);
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

}
