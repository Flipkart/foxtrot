package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV2;

/***
 Created by nitish.goyal on 25/09/18
 ***/
public interface EventProcessingService {

    EventProcessingResponseV2 process(EventProcessingRequest eventProcessingRequest) throws FoxtrotException;
}
