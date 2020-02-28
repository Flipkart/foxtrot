package com.flipkart.foxtrot.core.funnel.model.response;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Getter
@AllArgsConstructor
public class EventProcessingResponseV1 implements Serializable {

    private List<FunnelEventResponseV1> funnelEventResponses;

    private int responseHashCode;

    public EventProcessingResponseV1(List<FunnelEventResponseV1> funnelEventResponses) {
        this.funnelEventResponses = funnelEventResponses;
        this.responseHashCode = this.hashCode();
    }

    public EventProcessingResponseV1(int responseHashCode) {
        this.responseHashCode = responseHashCode;
    }

    public EventProcessingResponseV1() {
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
        EventProcessingResponseV1 eventProcessingResponseV1 = (EventProcessingResponseV1)obj;
        return (eventProcessingResponseV1.funnelEventResponses.equals(this.funnelEventResponses));
    }
}
