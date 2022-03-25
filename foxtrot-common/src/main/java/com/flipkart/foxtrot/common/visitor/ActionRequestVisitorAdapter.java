package com.flipkart.foxtrot.common.visitor;

import com.flipkart.foxtrot.common.ActionRequestVisitor;
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
import lombok.Getter;

public abstract class ActionRequestVisitorAdapter<T> implements ActionRequestVisitor<T> {

    @Getter
    final T defaultValue;

    protected ActionRequestVisitorAdapter(T defaultValue) {
        this.defaultValue = defaultValue;
    }


    public T visit(GroupRequest request) {
        return defaultValue;
    }

    public T visit(Query request) {
        return defaultValue;
    }

    public T visit(StatsRequest request) {
        return defaultValue;
    }

    public T visit(StatsTrendRequest request) {
        return defaultValue;
    }

    public T visit(TrendRequest request) {
        return defaultValue;
    }

    public T visit(DistinctRequest request) {
        return defaultValue;
    }

    public T visit(HistogramRequest request) {
        return defaultValue;
    }

    public T visit(MultiTimeQueryRequest request) {
        return defaultValue;
    }

    public T visit(MultiQueryRequest request) {
        return defaultValue;
    }

    public T visit(CountRequest request) {
        return defaultValue;
    }

    public T visit(GeoAggregationRequest request) {
        return defaultValue;
    }
}
