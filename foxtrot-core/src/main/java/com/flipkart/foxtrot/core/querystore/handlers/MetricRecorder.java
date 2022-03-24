package com.flipkart.foxtrot.core.querystore.handlers;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.visitor.ActionRequestFilterVisitor;
import com.flipkart.foxtrot.core.querystore.ActionEvaluationResponse;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.util.MetricUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Slf4j
public class MetricRecorder implements ActionExecutionObserver {

    @Override
    public void postExecution(ActionEvaluationResponse response) {
        if (null == response || null == response.getExecutedAction()) {
            return;
        }
        final ActionRequest request = response.getRequest();
        final String metricKey = response.getExecutedAction()
                .getMetricKey();

        registerFilterUsageMetrics(request);

        if (null == response.getException()) {
            MetricUtil.getInstance()
                    .registerActionFailure(metricKey, response.getElapsedTime());
        }
        if (response.isCached()) {
            MetricUtil.getInstance()
                    .registerActionCacheHit(request.getOpcode());
        } else {
            MetricUtil.getInstance()
                    .registerActionCacheMiss(request.getOpcode());
        }
        MetricUtil.getInstance()
                .registerActionSuccess(metricKey, response.getElapsedTime());

    }

    private void registerFilterUsageMetrics(ActionRequest request) {
        try {
            Map<String, List<Filter>> tableVsFilters = request.accept(new ActionRequestFilterVisitor());

            log.info("tableVsFilters: {}", tableVsFilters);
            tableVsFilters.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .forEach(filter -> MetricUtil.getInstance()
                            .registerFilterUsage(filter.getOperator()));

            tableVsFilters.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(filter -> Arrays.asList(FilterOperator.contains, FilterOperator.wildcard)
                            .contains(filter.getOperator()))
                    .findFirst()
                    .ifPresent(filter -> log.info("Action request found with contains/wildcard filter :{}",
                            JsonUtils.toJson(request)));

            tableVsFilters.forEach((table, filters) -> filters.forEach(filter -> MetricUtil.getInstance()
                    .registerFilterUsage(table, filter.getOperator())));


        } catch (Exception e) {
            log.error(
                    "Error while registering metrics for filter usage in postExecution for action request: {}, error: ",
                    request, e);
        }
    }

}
