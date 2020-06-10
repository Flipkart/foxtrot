package com.flipkart.foxtrot.common;

import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.trend.TrendResponse;

public interface ResponseVisitor<T> {

    T visit(GroupResponse groupResponse);

    T visit(HistogramResponse histogramResponse);

    T visit(QueryResponse queryResponse);

    T visit(StatsResponse statsResponse);

    T visit(StatsTrendResponse statsTrendResponse);

    T visit(TrendResponse trendResponse);

    T visit(CountResponse countResponse);

    T visit(DistinctResponse distinctResponse);

    T visit(MultiQueryResponse multiQueryResponse);

    T visit(MultiTimeQueryResponse multiTimeQueryResponse);
}
