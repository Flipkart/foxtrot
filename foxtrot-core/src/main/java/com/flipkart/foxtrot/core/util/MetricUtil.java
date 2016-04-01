package com.flipkart.foxtrot.core.util;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Created by rishabh.goyal on 05/09/15.
 */
public class MetricUtil {

    private static final MetricRegistry metrics;
    private static final MetricUtil metricsHelper;

    private static final String packagePrefix = "com.flipkart.foxtrot.core";
    private static final String actionMetricPrefix = "action";

    static {
        metrics = new MetricRegistry();
        metricsHelper = new MetricUtil();
    }

    public static MetricUtil getInstance() {
        return metricsHelper;
    }

    private MetricUtil() {
        JmxReporter.forRegistry(metrics).convertRatesTo(TimeUnit.MINUTES).build().start();
    }

    public void registerActionCacheHit(String opcode, String metricKey) {
        registerActionCacheOperation(opcode, metricKey, "success");
    }

    public void registerActionCacheMiss(String opcode, String metricKey) {
        registerActionCacheOperation(opcode, metricKey, "failure");
    }

    private void registerActionCacheOperation(String opcode, String metricKey, String status) {
        metrics.meter(String.format("%s.%s.cache.%s", packagePrefix, actionMetricPrefix, status)).mark();
        metrics.meter(String.format("%s.%s.%s.cache.%s", packagePrefix, actionMetricPrefix, opcode, status)).mark();
        metrics.meter(String.format("%s.%s.%s.%s.cache.%s", packagePrefix, actionMetricPrefix, opcode, metricKey, status)).mark();
    }

    public void registerActionSuccess(String opcode, String metricKey, long duration) {
        registerActionOperation(opcode, metricKey, "success", duration);
    }

    public void registerActionFailure(String opcode, String metricKey, long duration) {
        registerActionOperation(opcode, metricKey, "failure", duration);
    }

    private void registerActionOperation(String opcode, String metricKey, String status, long duration) {
        metrics.timer(String.format("%s.%s.%s", packagePrefix, actionMetricPrefix, opcode))
                .update(duration, TimeUnit.MILLISECONDS);
        metrics.timer(String.format("%s.%s.%s.%s", packagePrefix, actionMetricPrefix, opcode, metricKey))
                .update(duration, TimeUnit.MILLISECONDS);
        metrics.timer(String.format("%s.%s.%s.%s.%s", packagePrefix, actionMetricPrefix, opcode, metricKey, status))
                .update(duration, TimeUnit.MILLISECONDS);
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

}
