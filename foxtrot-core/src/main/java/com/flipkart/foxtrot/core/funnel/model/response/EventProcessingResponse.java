package com.flipkart.foxtrot.core.funnel.model.response;

import com.flipkart.foxtrot.core.funnel.model.FunnelEventResponse;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Getter
@AllArgsConstructor
public class EventProcessingResponse implements Serializable {

    private List<FunnelEventResponse> funnelEventResponses;

    private int responseHashCode;

    public EventProcessingResponse(List<FunnelEventResponse> funnelEventResponses) {
        this.funnelEventResponses = funnelEventResponses;
        this.responseHashCode = this.hashCode();
    }

    public EventProcessingResponse(int responseHashCode) {
        this.responseHashCode = responseHashCode;
    }

    public EventProcessingResponse() {
        this.responseHashCode = this.hashCode();
    }

    @Override
    public int hashCode() {
        int result = 3;
        if(funnelEventResponses == null || funnelEventResponses.isEmpty()) {
            result = 11 * result;
        } else {
            result = 11 * result + funnelEventResponses.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || obj.getClass() != this.getClass())
            return false;
        EventProcessingResponse eventProcessingResponse = (EventProcessingResponse)obj;
        return (eventProcessingResponse.funnelEventResponses.equals(this.funnelEventResponses));
    }
}
