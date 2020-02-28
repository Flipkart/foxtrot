package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;

public interface FunnelExtrapolationService {

    ActionResponse extrapolateResponse(ActionRequest actionRequest, ActionResponse originalResponse);
}
