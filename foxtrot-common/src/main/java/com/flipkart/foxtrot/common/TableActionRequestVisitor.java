package com.flipkart.foxtrot.common;

import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;

/***
 Created by mudit.g on Mar, 2019
 ***/
public class TableActionRequestVisitor implements ActionRequestVisitor<String> {

    public String visit(GroupRequest request) {
        return request.getTable();
    }

    public String visit(Query request) {
        return request.getTable();
    }

    public String visit(StatsRequest request) {
        return request.getTable();
    }

    public String visit(StatsTrendRequest request) {
        return request.getTable();
    }

    public String visit(TrendRequest request) {
        return request.getTable();
    }

    public String visit(DistinctRequest request) {
        return request.getTable();
    }

    public String visit(HistogramRequest request) {
        return request.getTable();
    }

    public String visit(MultiTimeQueryRequest request) {
        return null;
    }

    public String visit(MultiQueryRequest request) {
        return null;
    }

    public String visit(CountRequest request) {
        return request.getTable();
    }

    @Override
    public String visit(GeoAggregationRequest request) {
        return request.getTable();
    }
}
