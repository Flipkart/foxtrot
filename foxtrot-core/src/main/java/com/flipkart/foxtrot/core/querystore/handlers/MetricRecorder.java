package com.flipkart.foxtrot.core.querystore.handlers;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.querystore.ActionEvaluationResponse;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.util.MetricUtil;

/**
 *
 */
public class MetricRecorder implements ActionExecutionObserver {
    @Override
    public void postExecution(ActionEvaluationResponse response) {
        if(null == response || null == response.getExecutedAction()) {
            return;
        }
        final ActionRequest request = response.getRequest();
        final String metricKey = response.getExecutedAction().getMetricKey();
        if(null == response.getException()) {
            MetricUtil.getInstance()
                    .registerActionFailure(request.getOpcode(), metricKey, response.getElapsedTime());
        }
        if(response.isCached()) {
            MetricUtil.getInstance().registerActionCacheHit(request.getOpcode(), metricKey);
        }
        else {
            MetricUtil.getInstance().registerActionCacheMiss(request.getOpcode(), metricKey);
        }
        MetricUtil.getInstance()
                .registerActionSuccess(request.getOpcode(), metricKey, response.getElapsedTime());
    }

}
