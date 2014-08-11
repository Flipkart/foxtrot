package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;

import java.util.Map;

/**
 * Created by rishabh.goyal on 07/08/14.
 */
public class StatsResponse implements ActionResponse {
    private Map<String, Object> result;

    public StatsResponse() {

    }

    public StatsResponse(Map<String, Object> result) {
        this.result = result;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }
}
