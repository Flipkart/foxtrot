package com.flipkart.foxtrot.common.visitor;

import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;

import java.util.Objects;

public class CountPrecisionThresholdVisitorAdapter extends ActionRequestVisitorAdapter<Integer> {

    public CountPrecisionThresholdVisitorAdapter(Integer defaultValue) {
        super(defaultValue);
    }

    @Override
    public Integer visit(CountRequest request) {
        return Objects.isNull(request.getPrecision())
                ? defaultValue
                : request.getPrecision()
                .getPrecisionThreshold();
    }

    @Override
    public Integer visit(GroupRequest request) {
        return Objects.isNull(request.getPrecision())
                ? defaultValue
                : request.getPrecision()
                .getPrecisionThreshold();
    }


    @Override
    public Integer visit(HistogramRequest request) {
        return Objects.isNull(request.getPrecision())
                ? defaultValue
                : request.getPrecision()
                .getPrecisionThreshold();
    }


    @Override
    public Integer visit(TrendRequest request) {
        return Objects.isNull(request.getPrecision())
                ? defaultValue
                : request.getPrecision()
                .getPrecisionThreshold();
    }

}
