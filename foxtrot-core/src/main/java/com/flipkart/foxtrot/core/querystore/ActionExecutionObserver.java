package com.flipkart.foxtrot.core.querystore;

/**
 *
 */
public interface ActionExecutionObserver {
    void handleExecution(ActionEvaluationResponse response);
}
