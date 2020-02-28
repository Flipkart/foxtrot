package com.flipkart.foxtrot.core.funnel.model.response;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Getter
@Builder
@AllArgsConstructor
public class EventProcessingResponseV2 implements Serializable {

    private List<FunnelEventResponseV2> funnelEventResponses;

    private int responseHashCode;

    public EventProcessingResponseV2(List<FunnelEventResponseV2> funnelEventResponses) {
        this.funnelEventResponses = funnelEventResponses;
        this.responseHashCode = this.hashCode();
    }

    public EventProcessingResponseV2(int responseHashCode) {
        this.responseHashCode = responseHashCode;
    }

    public EventProcessingResponseV2() {
        this.responseHashCode = this.hashCode();
    }

    @Override
    public int hashCode() {
        int result = 3;
        if (funnelEventResponses == null || funnelEventResponses.isEmpty()) {
            result = 11 * result;
        } else {
            result = 11 * result + funnelEventResponses.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        EventProcessingResponseV2 processingResponseV2 = (EventProcessingResponseV2) obj;
        return (processingResponseV2.funnelEventResponses.equals(this.funnelEventResponses));
    }
}
