package com.flipkart.foxtrot.core.funnel.model.visitor;

import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunnelExtrapolationValidator implements ActionRequestVisitor<Boolean> {

    private static final List<Stat> VALID_STATS_FOR_EXTRAPOLATION = Arrays.asList(
            Stat.COUNT, Stat.SUM, Stat.SUM_OF_SQUARES);

    @Override
    public Boolean visit(GroupRequest groupRequest) {
        return Strings.isNullOrEmpty(groupRequest.getUniqueCountOn());
    }

    @Override
    public Boolean visit(Query queryRequest) {
        return false;
    }

    @Override
    public Boolean visit(StatsRequest statsRequest) {
        return !CollectionUtils.isNullOrEmpty(statsRequest.getStats()) && statsRequest.getStats()
                .stream()
                .anyMatch(VALID_STATS_FOR_EXTRAPOLATION::contains);
    }

    @Override
    public Boolean visit(StatsTrendRequest statsTrendRequest) {
        return !CollectionUtils.isNullOrEmpty(statsTrendRequest.getStats()) && statsTrendRequest.getStats()
                .stream()
                .anyMatch(VALID_STATS_FOR_EXTRAPOLATION::contains);
    }

    @Override
    public Boolean visit(TrendRequest trendRequest) {
        return Strings.isNullOrEmpty(trendRequest.getUniqueCountOn());
    }

    @Override
    public Boolean visit(DistinctRequest distinctRequest) {
        return false;
    }

    @Override
    public Boolean visit(HistogramRequest histogramRequest) {
        return Strings.isNullOrEmpty(histogramRequest.getUniqueCountOn());
    }

    @Override
    public Boolean visit(MultiTimeQueryRequest multiTimeQueryRequest) {
        return multiTimeQueryRequest.getActionRequest().accept(new FunnelExtrapolationValidator());
    }

    @Override
    public Boolean visit(MultiQueryRequest multiQueryRequest) {
        return multiQueryRequest.getRequests()
                .values()
                .stream()
                .anyMatch(actionRequest -> actionRequest.accept(new FunnelExtrapolationValidator()));
    }

    @Override
    public Boolean visit(CountRequest countRequest) {
        return !countRequest.isDistinct();
    }

}
