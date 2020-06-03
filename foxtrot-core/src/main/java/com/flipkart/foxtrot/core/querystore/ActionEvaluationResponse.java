package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.core.common.Action;
import lombok.Data;


/**
 *
 */
@Data
public class ActionEvaluationResponse {

    private final Action executedAction;
    private final ActionRequest request;
    private final ActionResponse response;
    private final FoxtrotException exception;
    private final long elapsedTime;
    private final boolean cached;

    private ActionEvaluationResponse(Action executedAction,
                                     ActionRequest request,
                                     ActionResponse response,
                                     FoxtrotException exception,
                                     long elapsedTime,
                                     boolean cached) {
        this.executedAction = executedAction;
        this.request = request;
        this.response = response;
        this.exception = exception;
        this.elapsedTime = elapsedTime;
        this.cached = cached;
    }

    public static ActionEvaluationResponse success(final Action executedAction,
                                                   final ActionRequest request,
                                                   final ActionResponse response,
                                                   long elapsedTime,
                                                   boolean cached) {
        return new ActionEvaluationResponse(executedAction, request, response, null, elapsedTime, cached);
    }

    public static ActionEvaluationResponse failure(final Action executedAction,
                                                   final ActionRequest request,
                                                   final FoxtrotException exception,
                                                   long elapsedTime) {
        return new ActionEvaluationResponse(executedAction, request, null, exception, elapsedTime, false);
    }
}
