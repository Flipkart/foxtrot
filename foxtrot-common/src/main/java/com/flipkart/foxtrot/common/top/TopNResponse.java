package com.flipkart.foxtrot.common.top;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 28/02/15.
 */
public class TopNResponse implements ActionResponse {

    private Map<String, List<ValueCount>> data = new HashMap<String, List<ValueCount>>();

    public TopNResponse() {
    }

    public TopNResponse(Map<String, List<ValueCount>> data) {
        this.data = data;
    }

    public Map<String, List<ValueCount>> getData() {
        return data;
    }

    public void setData(Map<String, List<ValueCount>> data) {
        this.data = data;
    }

    public void addFieldData(String field, List<ValueCount> data) {
        this.data.put(field, data);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("data", data)
                .toString();
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }
}
