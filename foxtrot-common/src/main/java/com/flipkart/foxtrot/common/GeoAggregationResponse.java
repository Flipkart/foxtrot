package com.flipkart.foxtrot.common;

import lombok.Getter;

import java.util.Map;

@Getter
public class GeoAggregationResponse extends ActionResponse {

    private Map<String, Object> metricByGrid;

    protected GeoAggregationResponse() {
        super(Opcodes.GEO_AGGREGATION);
    }

    public GeoAggregationResponse(Map<String, Object> metricByGrid) {
        super();
        this.metricByGrid = metricByGrid;
    }

    @Override
    public <T> T accept(ResponseVisitor<T> visitor) {
        return null;
    }

}
