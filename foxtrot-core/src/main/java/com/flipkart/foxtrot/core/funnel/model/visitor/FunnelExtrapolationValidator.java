package com.flipkart.foxtrot.core.funnel.model.visitor;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.GeoAggregationRequest;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils.printFunnelApplicableLog;
import static com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils.printFunnelNotApplicableLog;

@Slf4j
/*
  Returns true if funnel extrapolation is applicable for a given action request
 */ public class FunnelExtrapolationValidator implements ActionRequestVisitor<Boolean> {

    private static final List<Stat> VALID_STATS_FOR_EXTRAPOLATION = Arrays.asList(Stat.COUNT, Stat.SUM,
            Stat.SUM_OF_SQUARES);

    @Override
    public Boolean visit(GroupRequest groupRequest) {
        return isFunnelQuery(groupRequest, actionRequest -> Strings.isNullOrEmpty(groupRequest.getUniqueCountOn()));
    }

    @Override
    public Boolean visit(Query queryRequest) {
        return false;
    }

    @Override
    public Boolean visit(StatsRequest statsRequest) {
        return isFunnelQuery(statsRequest,
                actionRequest -> !CollectionUtils.isNullOrEmpty(statsRequest.getStats()) && statsRequest.getStats()
                        .stream()
                        .anyMatch(VALID_STATS_FOR_EXTRAPOLATION::contains));

    }

    @Override
    public Boolean visit(StatsTrendRequest statsTrendRequest) {
        return isFunnelQuery(statsTrendRequest,
                actionRequest -> !CollectionUtils.isNullOrEmpty(statsTrendRequest.getStats())
                        && statsTrendRequest.getStats()
                        .stream()
                        .anyMatch(VALID_STATS_FOR_EXTRAPOLATION::contains));
    }

    @Override
    public Boolean visit(TrendRequest trendRequest) {
        return isFunnelQuery(trendRequest, actionRequest -> Strings.isNullOrEmpty(trendRequest.getUniqueCountOn()));
    }

    @Override
    public Boolean visit(DistinctRequest distinctRequest) {
        return false;
    }

    @Override
    public Boolean visit(HistogramRequest histogramRequest) {
        return isFunnelQuery(histogramRequest,
                actionRequest -> Strings.isNullOrEmpty(histogramRequest.getUniqueCountOn()));
    }

    @Override
    public Boolean visit(MultiTimeQueryRequest multiTimeQueryRequest) {
        return multiTimeQueryRequest.getActionRequest()
                .accept(new FunnelExtrapolationValidator());
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
        return isFunnelQuery(countRequest, actionRequest -> !countRequest.isDistinct());
    }

    @Override
    public Boolean visit(GeoAggregationRequest request) {
        return false;
    }

    private <T extends ActionRequest> Boolean isFunnelQuery(ActionRequest actionRequest,
                                                            Predicate<T> condition) {
        if (!condition.test((T) actionRequest)) {
            printFunnelNotApplicableLog(actionRequest);
            return false;
        }

        // For now, we're only considering the actionRequest to be valid for extrapolation
        // if funnelId is provided in one of the equals filter

        // In future, we need to figure out the funnel id which will be used for extrapolation using eventType
        // Reverse lookup eventType to find all funnels this eventType is tagged to
        // and then pick the last active funnel assigned to this eventType

        Optional<Long> funnelIdOptional = FunnelExtrapolationUtils.extractFunnelId(actionRequest);
        if (funnelIdOptional.isPresent()) {
            printFunnelApplicableLog(actionRequest, funnelIdOptional.get());
            return true;
        }
        printFunnelNotApplicableLog(actionRequest);
        return false;
    }

}
