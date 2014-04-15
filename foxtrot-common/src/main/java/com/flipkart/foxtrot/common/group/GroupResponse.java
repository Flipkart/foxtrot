package com.flipkart.foxtrot.common.group;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.spi.AnalyticsResponse;

import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 5:07 PM
 */
@AnalyticsResponse("group")
public class GroupResponse implements ActionResponse {
    private Map<String, Object> result;

    public GroupResponse() {
    }

    public GroupResponse(Map<String, Object> result) {
        this.result = result;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }
}
