package com.flipkart.foxtrot.common.visitor;

import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;

public class ConsoleIdVisitorAdapter extends ActionRequestVisitorAdapter<String> {

    public ConsoleIdVisitorAdapter(String defaultValue) {
        super(defaultValue);
    }

    public String visit(GroupRequest request) {
        return request.getConsoleId();
    }

    public String visit(Query request) {
        return defaultValue;
    }

    public String visit(StatsRequest request) {
        return request.getConsoleId();
    }

    public String visit(StatsTrendRequest request) {
        return request.getConsoleId();
    }

    public String visit(TrendRequest request) {
        return request.getConsoleId();
    }

    public String visit(DistinctRequest request) {
        return defaultValue;
    }

    public String visit(HistogramRequest request) {
        return request.getConsoleId();
    }

    public String visit(MultiTimeQueryRequest request) {
        return defaultValue;
    }

    public String visit(MultiQueryRequest request) {
        return request.getConsoleId();
    }

    public String visit(CountRequest request) {
        return request.getConsoleId();
    }

}
