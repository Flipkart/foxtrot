package com.flipkart.foxtrot.common;

import com.flipkart.foxtrot.common.query.Filter;

import java.util.List;

public class TrendsRequest extends ActionRequest {


    public TrendsRequest(String opcode, List<Filter> filters, String fieldname, String tablename, String period , List<String> values) {
        super(opcode, filters);
        this.fieldname = fieldname;
        this.tablename = tablename;
        this.period = period;
        this.values=values;
    }

    private  String fieldname;
    private  String tablename ;
    private  String period;
    private List<String> values;


}
