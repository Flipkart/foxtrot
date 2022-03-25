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
public interface ActionRequestVisitor<T> {

    T visit(GroupRequest request);

    T visit(Query request);

    T visit(StatsRequest request);

    T visit(StatsTrendRequest request);

    T visit(TrendRequest request);

    T visit(DistinctRequest request);

    T visit(HistogramRequest request);

    T visit(MultiTimeQueryRequest request);

    T visit(MultiQueryRequest request);

    T visit(CountRequest request);

    T visit(GeoAggregationRequest request);
}
