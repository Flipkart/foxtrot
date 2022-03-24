package com.flipkart.foxtrot.core.querystore.handlers;

import com.flipkart.foxtrot.common.exception.SerDeException;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.querystore.ActionEvaluationResponse;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;

/**
 *
 */
@Slf4j
@Singleton
public class SlowQueryReporter implements ActionExecutionObserver {

    public static final String SLOW_QUERY_EMAIL_TEMPLATE_ID = "slow_query_reporter";
    private final QueryConfig queryConfig;

    @Inject
    public SlowQueryReporter(final QueryConfig queryConfig) {
        this.queryConfig = queryConfig;
    }

    @Override
    public void postExecution(ActionEvaluationResponse evaluationResponse) {
        if (null == evaluationResponse || null != evaluationResponse.getException() || evaluationResponse.isCached()) {
            return;
        }
        if (evaluationResponse.getElapsedTime() > queryConfig.getSlowQueryThresholdMs()) {
            String query = "";
            try {
                query = JsonUtils.toJson(evaluationResponse.getRequest());
                log.warn("SLOW_QUERY: Time: {} ms , cache key: {}, Query: {}", evaluationResponse.getElapsedTime(),
                        evaluationResponse.getExecutedAction()
                                .getRequestCacheKey(), query);
            } catch (SerDeException e) {
                log.error("Error serializing slow query", e);
            }
        }
    }
}
