package com.flipkart.foxtrot.core.funnel.model.request;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Data
@Builder
public class EventProcessingRequest {

    private Map<String, String> userSpecificFields;
    private Map<String, String> appSpecificFields;
    private Map<String, String> deviceSpecificFields;

    private long timeStamp;

    private int responseHashCode;
}
