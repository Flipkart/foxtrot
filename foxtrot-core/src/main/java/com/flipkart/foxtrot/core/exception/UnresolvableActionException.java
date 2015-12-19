package com.flipkart.foxtrot.core.exception;

import com.flipkart.foxtrot.common.ActionRequest;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class UnresolvableActionException extends FoxtrotException {

    private ActionRequest actionRequest;

    public UnresolvableActionException(ActionRequest actionRequest) {
        super(ErrorCode.UNRESOLVABLE_OPERATION);
    }

    public ActionRequest getActionRequest() {
        return actionRequest;
    }

    public void setActionRequest(ActionRequest actionRequest) {
        this.actionRequest = actionRequest;
    }
}
