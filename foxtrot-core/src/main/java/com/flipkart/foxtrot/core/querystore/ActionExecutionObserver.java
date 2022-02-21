package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.ActionRequest;

/**
 *
 */
public interface ActionExecutionObserver {
    default void preExecution(ActionRequest request) {
    }

    default void postExecution(ActionEvaluationResponse response) {
    }
}
