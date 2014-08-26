package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionResponse;

import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendResponse implements ActionResponse {

    private List<StatsTrendValue> result;

    public StatsTrendResponse() {

    }

    public StatsTrendResponse(List<StatsTrendValue> statsList) {
        this.result = statsList;
    }

    public List<StatsTrendValue> getResult() {
        return result;
    }

    public void setResult(List<StatsTrendValue> result) {
        this.result = result;
    }
}
