package com.flipkart.foxtrot.core.funnel.model.visitor;

import static com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.ResponseVisitor;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse.Count;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendValue;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunnelExtrapolationResponseVisitor implements ResponseVisitor<ActionResponse> {

    private static final String EVENT_TYPE = "eventType";

    private final ActionRequest actionRequest;

    private final QueryExecutor queryExecutor;

    private final Long funnelId;

    private final BaseFunnelEventConfig funnelEventConfig;

    private final TableActionRequestVisitor tableActionRequestVisitor;

    public FunnelExtrapolationResponseVisitor(final Long funnelId,
                                              final ActionRequest actionRequest,
                                              final QueryExecutor queryExecutor,
                                              final BaseFunnelEventConfig funnelEventConfig) {
        this.funnelId = funnelId;
        this.actionRequest = actionRequest;
        this.queryExecutor = queryExecutor;
        this.funnelEventConfig = funnelEventConfig;
        this.tableActionRequestVisitor = new TableActionRequestVisitor();
    }

    public ActionResponse visit(GroupResponse groupResponse) {
        double extrapolationFactor = computeExtrapolationFactor();
        extrapolateGroupResponse(extrapolationFactor, groupResponse.getResult());
        return groupResponse;
    }

    public ActionResponse visit(HistogramResponse histogramResponse) {
        HistogramRequest histogramRequest = (HistogramRequest) this.actionRequest;
        List<Count> extrapolationFactors = computeExtrapolationFactors(histogramRequest.getTable(),
                histogramRequest.getField(), histogramRequest.getPeriod());

        // assuming order is preserved in histogramResponse and calculated extrapolation factors
        if (CollectionUtils.isNotEmpty(histogramResponse.getCounts()) && histogramResponse.getCounts()
                .size() == extrapolationFactors.size()) {
            for (int i = 0; i < histogramResponse.getCounts()
                    .size(); i++) {
                Count count = histogramResponse.getCounts()
                        .get(i);
                count.setCount(count.getCount() * extrapolationFactors.get(i)
                        .getCount());
            }
        }
        return histogramResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(QueryResponse queryResponse) {
        return queryResponse;
    }

    public ActionResponse visit(StatsResponse statsResponse) {
        double extrapolationFactor = computeExtrapolationFactor();
        Map<String, Number> originalStats = statsResponse.getResult()
                .getStats();
        statsResponse.getResult()
                .setStats(extrapolateStats(extrapolationFactor, originalStats));

        List<BucketResponse<StatsValue>> buckets = statsResponse.getBuckets();
        extrapolateStatsBuckets(extrapolationFactor, buckets);

        return statsResponse;
    }

    public ActionResponse visit(StatsTrendResponse statsTrendResponse) {
        StatsTrendRequest statsTrendRequest = (StatsTrendRequest) actionRequest;

        List<Count> extrapolationFactors = computeExtrapolationFactors(statsTrendRequest.getTable(),
                statsTrendRequest.getTimestamp(), statsTrendRequest.getPeriod());

        List<StatsTrendValue> statsTrendValues = statsTrendResponse.getResult();
        extrapolateStatsTrendValues(extrapolationFactors, statsTrendValues);

        List<BucketResponse<List<StatsTrendValue>>> buckets = statsTrendResponse.getBuckets();
        extrapolateStatsTrendBuckets(extrapolationFactors, buckets);

        return statsTrendResponse;
    }

    public ActionResponse visit(TrendResponse trendResponse) {
        TrendRequest trendRequest = (TrendRequest) actionRequest;

        List<Count> extrapolationFactors = computeExtrapolationFactors(trendRequest.getTable(),
                trendRequest.getTimestamp(), trendRequest.getPeriod());

        if (CollectionUtils.isNotEmpty(trendResponse.getTrends())) {
            trendResponse.getTrends()
                    .values()
                    .forEach(counts -> {
                        if (counts.size() == extrapolationFactors.size()) {
                            for (int i = 0; i < counts.size(); i++) {
                                long originalCount = counts.get(i)
                                        .getCount();
                                counts.get(i)
                                        .setCount(originalCount * extrapolationFactors.get(i)
                                                .getCount());
                            }
                        }
                    });
        }
        return trendResponse;
    }

    public ActionResponse visit(CountResponse countResponse) {
        double extrapolationFactor = computeExtrapolationFactor();
        countResponse.setCount((long) (countResponse.getCount() * extrapolationFactor));
        return countResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(DistinctResponse distinctResponse) {
        return distinctResponse;
    }

    public ActionResponse visit(MultiQueryResponse multiQueryResponse) {
        MultiQueryRequest multiQueryRequest = (MultiQueryRequest) actionRequest;
        if (CollectionUtils.isNotEmpty(multiQueryResponse.getResponses())) {
            for (Map.Entry<String, ActionResponse> entry : multiQueryResponse.getResponses()
                    .entrySet()) {
                FunnelExtrapolationResponseVisitor funnelExtrapolationResponseVisitor = new FunnelExtrapolationResponseVisitor(
                        funnelId, multiQueryRequest.getRequests()
                        .get(entry.getKey()), queryExecutor, funnelEventConfig);
                entry.setValue(entry.getValue()
                        .accept(funnelExtrapolationResponseVisitor));
            }
        }
        return multiQueryResponse;
    }

    public ActionResponse visit(MultiTimeQueryResponse multiTimeQueryResponse) {
        return multiTimeQueryResponse;
    }

    private Map<String, Object> extrapolateGroupResponse(double extrapolationFactor,
                                                         Map<String, Object> groupResponseResult) {
        for (Map.Entry<String, Object> entry : groupResponseResult.entrySet()) {
            if (entry.getValue() instanceof Long) {
                entry.setValue((long) (((Long) entry.getValue()) * extrapolationFactor));
            } else {
                Map<String, Object> map = (Map<String, Object>) entry.getValue();
                entry.setValue(extrapolateGroupResponse(extrapolationFactor, map));
            }
        }
        return groupResponseResult;
    }

    private void extrapolateStatsBuckets(double extrapolationFactor,
                                         List<BucketResponse<StatsValue>> buckets) {
        if (CollectionUtils.isNotEmpty(buckets)) {
            for (BucketResponse<StatsValue> bucketResponse : buckets) {
                Map<String, Number> originalBucketStats = bucketResponse.getResult()
                        .getStats();
                bucketResponse.getResult()
                        .setStats(extrapolateStats(extrapolationFactor, originalBucketStats));
                extrapolateStatsBuckets(extrapolationFactor, bucketResponse.getBuckets());
            }
        }
    }

    private Map<String, Number> extrapolateStats(double extrapolationFactor,
                                                 Map<String, Number> originalStats) {
        if (CollectionUtils.isNotEmpty(originalStats)) {
            Map<String, Number> extrapolatedStats = new HashMap<>(originalStats);
            for (Map.Entry<String, Number> entry : extrapolatedStats.entrySet()) {
                if (FunnelExtrapolationUtils.getValidStatsForExtrapolation()
                        .contains(entry.getKey())) {
                    entry.setValue(extrapolatedValue(entry.getKey(), entry.getValue(), extrapolationFactor));
                }
            }
            return extrapolatedStats;
        }
        return originalStats;
    }


    private void extrapolateStatsTrendBuckets(List<Count> extrapolationFactors,
                                              List<BucketResponse<List<StatsTrendValue>>> buckets) {
        if (CollectionUtils.isNotEmpty(buckets)) {
            for (BucketResponse<List<StatsTrendValue>> bucketResponse : buckets) {
                extrapolateStatsTrendValues(extrapolationFactors, bucketResponse.getResult());
                extrapolateStatsTrendBuckets(extrapolationFactors, bucketResponse.getBuckets());
            }
        }
    }

    private void extrapolateStatsTrendValues(List<Count> extrapolationFactors,
                                             List<StatsTrendValue> statsTrendValues) {
        if (CollectionUtils.isNotEmpty(statsTrendValues) && extrapolationFactors.size() == statsTrendValues.size()) {
            for (int i = 0; i < statsTrendValues.size(); i++) {
                Map<String, Number> originalStats = statsTrendValues.get(i)
                        .getStats();
                Map<String, Number> extrapolatedStats = extrapolateStats(extrapolationFactors.get(i)
                        .getCount(), originalStats);
                statsTrendValues.get(i)
                        .setStats(extrapolatedStats);
            }
        }
    }

    private double computeExtrapolationFactor() {
        String table = actionRequest.accept(tableActionRequestVisitor);
        long totalBaseEventCount = getTotalBaseEventCount(table);

        long baseEventCountForFunnelId = getBaseEventCountForFunnelId(funnelId, table);

        if (baseEventCountForFunnelId != 0) {
            return (double) totalBaseEventCount / baseEventCountForFunnelId;
        }
        return 1.0;
    }

    private Number extrapolatedValue(String key,
                                     Number value,
                                     double extrapolationFactor) {
        switch (key) {
            case Utils.COUNT:
                return (long) (value.longValue() * extrapolationFactor);
            case Utils.SUM:
            case Utils.SUM_OF_SQUARES:
                return value.doubleValue() * extrapolationFactor;
            default:
                return value;
        }
    }

    private long getBaseEventCountForFunnelId(long funnelId,
                                              String table) {
        CountRequest countRequest = buildBaseEventCountRequest(table);

        countRequest.getFilters()
                .add(funnelIdFilter(funnelId));

        return ((CountResponse) queryExecutor.execute(countRequest)).getCount();
    }

    private GreaterEqualFilter funnelIdFilter(long funnelId) {
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField(FUNNEL_ID_QUERY_FIELD);
        greaterEqualFilter.setTemporal(false);
        greaterEqualFilter.setValue(funnelId);
        return greaterEqualFilter;
    }

    private long getTotalBaseEventCount(String table) {
        CountRequest totalCountRequest = buildBaseEventCountRequest(table);

        return ((CountResponse) queryExecutor.execute(totalCountRequest)).getCount();
    }

    private CountRequest buildBaseEventCountRequest(String table) {
        CountRequest countRequest = CountRequest.builder()
                .table(table)
                .build();

        List<Filter> filters = baseEventFilter();

        countRequest.setFilters(filters);

        return countRequest;
    }

    private List<Filter> baseEventFilter() {
        List<Filter> filters = actionRequest.getFilters()
                .stream()
                .filter(Filter::isFilterTemporal)
                .collect(Collectors.toList());

        EqualsFilter equalsFilter = new EqualsFilter(EVENT_TYPE, funnelEventConfig.getEventType());
        filters.add(equalsFilter);
        return filters;
    }

    private List<Count> computeExtrapolationFactors(String table,
                                                    String field,
                                                    Period period) {
        List<Count> extrapolationFactors = new ArrayList<>();
        List<Count> totalBaseEventCounts = getTotalBaseEventCount(table, field, period).getCounts();
        List<Count> baseEventCountsForFunnelId = getBaseEventCountForFunnelId(table, field, period).getCounts();

        if (totalBaseEventCounts.size() != baseEventCountsForFunnelId.size()) {
            log.error("Counts different for total base event count and base event with "
                    + "funnel id count for actionRequest :{}", actionRequest);
            return extrapolationFactors;
        }

        List<Count> extrapolationCounts = new ArrayList<>();

        // Assuming order is preserved in totalBaseEventCounts histogram response and
        // baseEventCountsForFunnelId histogram response we can take ratio at same indices
        for (int i = 0; i < totalBaseEventCounts.size(); i++) {
            if (baseEventCountsForFunnelId.get(i)
                    .getCount() != 0) {
                long extrapolationFactor = (long) ((double) totalBaseEventCounts.get(i)
                        .getCount() / baseEventCountsForFunnelId.get(i)
                        .getCount());
                extrapolationCounts.add(new Count(totalBaseEventCounts.get(i)
                        .getPeriod(), extrapolationFactor));
            } else {
                extrapolationCounts.add(new Count(totalBaseEventCounts.get(i)
                        .getPeriod(), 1));
            }
        }
        log.info("Calculated extrapolation counts: {} for actionRequest:{}", extrapolationCounts, actionRequest);
        return extrapolationCounts;
    }

    private HistogramResponse getBaseEventCountForFunnelId(String table,
                                                           String field,
                                                           Period period) {
        List<Filter> filters = baseEventFilter();
        filters.add(funnelIdFilter(funnelId));

        HistogramRequest histogramRequest = new HistogramRequest(filters, table, field, null, period);

        return (HistogramResponse) queryExecutor.execute(histogramRequest);
    }

    private HistogramResponse getTotalBaseEventCount(String table,
                                                     String field,
                                                     Period period) {
        List<Filter> filters = baseEventFilter();

        HistogramRequest histogramRequest = new HistogramRequest(filters, table, field, null, period);

        return (HistogramResponse) queryExecutor.execute(histogramRequest);
    }
}
