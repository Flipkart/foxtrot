package com.flipkart.foxtrot.common.visitor;

import com.flipkart.foxtrot.common.GeoAggregationRequest;
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
import com.google.common.base.Strings;

public class ConsoleIdVisitorAdapter extends ActionRequestVisitorAdapter<String> {

    private static final String DEFAULT_CONSOLE_ID = "none";

    public ConsoleIdVisitorAdapter() {
        super(DEFAULT_CONSOLE_ID);
    }

    public String visit(GroupRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(Query request) {
        return defaultValue;
    }

    public String visit(StatsRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(StatsTrendRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(TrendRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(DistinctRequest request) {
        return defaultValue;
    }

    public String visit(HistogramRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(MultiTimeQueryRequest request) {
        return defaultValue;
    }

    public String visit(MultiQueryRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    public String visit(CountRequest request) {
        return Strings.isNullOrEmpty(request.getConsoleId())
                ? DEFAULT_CONSOLE_ID
                : request.getConsoleId();
    }

    @Override
    public String visit(GeoAggregationRequest request) {
        return DEFAULT_CONSOLE_ID;
    }

}
