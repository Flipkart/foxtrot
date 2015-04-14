package com.flipkart.foxtrot.common;

import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.trend.TrendResponse;

public interface ResponseVisitor {

    void visit(GroupResponse groupResponse);

    void visit(HistogramResponse histogramResponse);

    void visit(QueryResponse queryResponse);

    void visit(StatsResponse statsResponse);

    void visit(StatsTrendResponse statsTrendResponse);

    void visit(TrendResponse trendResponse);

    void visit(CountResponse countResponse);

    void visit(DistinctResponse distinctResponse);
}
