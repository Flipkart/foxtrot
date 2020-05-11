package com.flipkart.foxtrot.core.querystore.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.core.querystore.ActionEvaluationResponse;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class SlowQueryReporter implements ActionExecutionObserver {
    @Override
    public void postExecution(ActionEvaluationResponse response) {
        if(null == response || null != response.getException() || response.isCached()) {
            return;
        }
        if (response.getElapsedTime() > 1000) {
            try {
                String query = response.getExecutedAction().getObjectMapper().writeValueAsString(response.getRequest());
                log.warn("SLOW_QUERY: Time: {} ms Query: {}", response.getElapsedTime(), query);
            }
            catch (JsonProcessingException e) {
                log.error("Error serializing slow query", e);
            }
        }
    }
}
