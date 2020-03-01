package com.flipkart.foxtrot.core.funnel.model.visitor;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.stats.BucketResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunnelResponseExtrapolationVisitor implements ResponseVisitor<ActionResponse> {

    private static final String FUNNEL_ID_QUERY_FIELD = "eventData.funnelInfo.funnelId";

    private final ActionRequest actionRequest;

    private final QueryExecutor queryExecutor;

    private final BaseFunnelEventConfig funnelEventConfig;

    private final TableActionRequestVisitor tableActionRequestVisitor;

    private static final List<String> VALID_STATS_FOR_EXTRAPOLATION = Arrays.asList(
            Utils.COUNT, Utils.SUM, Utils.SUM_OF_SQUARES);

    public FunnelResponseExtrapolationVisitor(final ActionRequest actionRequest,
            final QueryExecutor queryExecutor,
            final BaseFunnelEventConfig funnelEventConfig) {
        this.actionRequest = actionRequest;
        this.queryExecutor = queryExecutor;
        this.funnelEventConfig = funnelEventConfig;
        this.tableActionRequestVisitor = new TableActionRequestVisitor();
    }

    public ActionResponse visit(GroupResponse groupResponse) {
        long extrapolationFactor = getExtrapolationFactor();
        extrapolateGroupResponse(extrapolationFactor, groupResponse.getResult());
        return groupResponse;
    }

    private Map<String, Object> extrapolateGroupResponse(long extrapolationFactor,
            Map<String, Object> groupResponseResult) {
        for (Map.Entry<String, Object> entry : groupResponseResult.entrySet()) {
            if (entry.getValue() instanceof Long) {
                entry.setValue((Long) entry.getValue() * extrapolationFactor);
            } else {
                Map<String, Object> map = (Map<String, Object>) entry.getValue();
                entry.setValue(extrapolateGroupResponse(extrapolationFactor, map));
            }
        }
        return groupResponseResult;
    }

    public ActionResponse visit(HistogramResponse histogramResponse) {
        return histogramResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(QueryResponse queryResponse) {
        return queryResponse;
    }

    public ActionResponse visit(StatsResponse statsResponse) {
        long extrapolationFactor = getExtrapolationFactor();
        Map<String, Number> originalStats = statsResponse.getResult().getStats();
        statsResponse.getResult().setStats(extrapolateStats(extrapolationFactor, originalStats));

        List<BucketResponse<StatsValue>> buckets = statsResponse.getBuckets();
        extrapolateStatsBuckets(extrapolationFactor, buckets);

        return statsResponse;
    }

    public ActionResponse visit(StatsTrendResponse statsTrendResponse) {
        return statsTrendResponse;
    }

    private void extrapolateStatsBuckets(long extrapolationFactor, List<BucketResponse<StatsValue>> buckets) {
        if (CollectionUtils.isNotEmpty(buckets)) {
            for (BucketResponse<StatsValue> bucketResponse : buckets) {
                Map<String, Number> originalBucketStats = bucketResponse.getResult().getStats();
                bucketResponse.getResult().setStats(extrapolateStats(extrapolationFactor, originalBucketStats));
                extrapolateStatsBuckets(extrapolationFactor, bucketResponse.getBuckets());
            }
        }
    }

    private Map<String, Number> extrapolateStats(long extrapolationFactor,
            Map<String, Number> originalStats) {
        if (CollectionUtils.isNotEmpty(originalStats)) {
            Map<String, Number> extrapolatedStats = new HashMap<>(originalStats);
            for (Map.Entry<String, Number> entry : extrapolatedStats.entrySet()) {
                if (VALID_STATS_FOR_EXTRAPOLATION.contains(entry.getKey())) {
                    entry.setValue(extrapolatedValue(entry.getKey(), entry.getValue(), extrapolationFactor));
                }
            }
            return extrapolatedStats;
        }
        return originalStats;
    }

    public ActionResponse visit(TrendResponse trendResponse) {
        return trendResponse;
    }

    public ActionResponse visit(CountResponse countResponse) {
        long extrapolationFactor = getExtrapolationFactor();
        countResponse.setCount(countResponse.getCount() * extrapolationFactor);
        return countResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(DistinctResponse distinctResponse) {
        return distinctResponse;
    }

    public ActionResponse visit(MultiQueryResponse multiQueryResponse) {
        return multiQueryResponse;
    }

    public ActionResponse visit(MultiTimeQueryResponse multiTimeQueryResponse) {
        return multiTimeQueryResponse;
    }

    private long extractFunnelId(ActionRequest actionRequest) {
        long funnelId = 0;

        // Extract funnel id if equals filter is applied on eventData.funnelInfo.funnelId
        try {
            Optional<Filter> funnelIdFilter = actionRequest.getFilters().stream()
                    .filter(filter -> (filter instanceof EqualsFilter)
                            && (filter.getField().equals(FUNNEL_ID_QUERY_FIELD))
                            && ((EqualsFilter) filter).getValue() instanceof String
                    )
                    .findFirst();
            if (funnelIdFilter.isPresent()) {
                funnelId = Long.parseLong((String) (((EqualsFilter) funnelIdFilter.get()).getValue()));
            }
        } catch (NumberFormatException ex) {
            log.error("Error while extracting funnel id from action request : {} ", actionRequest, ex);
        }

        // TODO: Extract funnelId from eventType when funnelId is not given in filter
        return funnelId;
    }

    private long getExtrapolationFactor() {
        long funnelId = extractFunnelId(actionRequest);
        return funnelId == 0 ? 1 : computeExtrapolationFactor(funnelId,
                actionRequest.accept(tableActionRequestVisitor));
    }

    private long computeExtrapolationFactor(long funnelId, String table) {
        long totalBaseEventCount = getTotalBaseEventCount(table);

        long baseEventCountForFunnelId = getBaseEventCountForFunnelId(funnelId, table);

        return (long) ((double) totalBaseEventCount / baseEventCountForFunnelId);
    }


    private Number extrapolatedValue(String key, Number value, long extrapolationFactor) {
        switch (key) {
            case Utils.COUNT:
                return value.longValue() * extrapolationFactor;
            case Utils.SUM:
            case Utils.SUM_OF_SQUARES:
                return value.doubleValue() * extrapolationFactor;
            default:
                return value;
        }
    }

    private long getBaseEventCountForFunnelId(long funnelId, String table) {
        CountRequest countRequest = CountRequest.builder()
                .table(table)
                .field(funnelEventConfig.getEventType())
                .build();
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField(FUNNEL_ID_QUERY_FIELD);
        greaterEqualFilter.setTemporal(false);
        greaterEqualFilter.setValue(funnelId);
        List<Filter> temporalFilters = actionRequest.getFilters().stream()
                .filter(Filter::isFilterTemporal)
                .collect(Collectors.toList());
        temporalFilters.add(greaterEqualFilter);
        countRequest.setFilters(temporalFilters);
        return ((CountResponse) queryExecutor.execute(countRequest)).getCount();
    }

    private long getTotalBaseEventCount(String table) {
        CountRequest totalCountRequest = CountRequest.builder()
                .table(table)
                .field(funnelEventConfig.getEventType())
                .build();
        List<Filter> temporalFilters = actionRequest.getFilters().stream()
                .filter(Filter::isFilterTemporal)
                .collect(Collectors.toList());
        totalCountRequest.setFilters(temporalFilters);
        return ((CountResponse) queryExecutor.execute(totalCountRequest)).getCount();
    }
}
