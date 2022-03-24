package com.flipkart.foxtrot.core.funnel.model.visitor;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.*;
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
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.stats.*;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.queryexecutor.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.DOT;
import static com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD;

@Slf4j
public class FunnelExtrapolationResponseVisitor implements ResponseVisitor<ActionResponse> {

    private static final String EVENT_TYPE = "eventType";

    private static final String CATEGORY = "eventData.category";

    private static final String TIMESTAMP = "_timestamp";

    private static final String DATE = "date";

    private static final String ORIGINAL_QUERY_RESPONSE = "Original Query Response: {}";
    private static final String EXTRAPOLATED_QUERY_RESPONSE = "Extrapolated Query Response: {}";

    private final ActionRequest actionRequest;

    private final QueryExecutor queryExecutor;

    private final TableActionRequestVisitor tableActionRequestVisitor;

    private final FunnelStore funnelStore;

    private final FunnelConfiguration funnelConfiguration;

    public FunnelExtrapolationResponseVisitor(final ActionRequest actionRequest,
                                              final QueryExecutor queryExecutor,
                                              final FunnelStore funnelStore,
                                              final FunnelConfiguration funnelConfiguration) {
        this.actionRequest = actionRequest;
        this.queryExecutor = queryExecutor;
        this.funnelConfiguration = funnelConfiguration;
        this.tableActionRequestVisitor = new TableActionRequestVisitor();
        this.funnelStore = funnelStore;
    }

    public ActionResponse visit(GroupResponse groupResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, groupResponse);

        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        double extrapolationFactor = computeExtrapolationFactor(applicableFunnel);

        extrapolateGroupResponse(extrapolationFactor, groupResponse.getResult());

        log.debug(EXTRAPOLATED_QUERY_RESPONSE, groupResponse);
        return groupResponse;
    }

    public ActionResponse visit(HistogramResponse histogramResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, histogramResponse);
        HistogramRequest histogramRequest = (HistogramRequest) actionRequest;

        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        List<HistogramValue> extrapolationFactors = computeExtrapolationFactors(histogramRequest.getTable(),
                histogramRequest.getField(), histogramRequest.getPeriod(), applicableFunnel);

        // assuming order is preserved in histogramResponse and calculated extrapolation factors
        if (CollectionUtils.isNotEmpty(histogramResponse.getCounts()) && histogramResponse.getCounts()
                .size() == extrapolationFactors.size()) {
            for (int i = 0; i < histogramResponse.getCounts()
                    .size(); i++) {
                Count count = histogramResponse.getCounts()
                        .get(i);
                count.setCount((long) (count.getCount() * extrapolationFactors.get(i)
                        .getValue()));
            }
        }

        log.debug(EXTRAPOLATED_QUERY_RESPONSE, histogramResponse);
        return histogramResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(QueryResponse queryResponse) {
        return queryResponse;
    }

    public ActionResponse visit(StatsResponse statsResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, statsResponse);

        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        double extrapolationFactor = computeExtrapolationFactor(applicableFunnel);
        Map<String, Number> originalStats = statsResponse.getResult()
                .getStats();
        statsResponse.getResult()
                .setStats(extrapolateStats(extrapolationFactor, originalStats));

        List<BucketResponse<StatsValue>> buckets = statsResponse.getBuckets();
        extrapolateStatsBuckets(extrapolationFactor, buckets);

        log.debug(EXTRAPOLATED_QUERY_RESPONSE, statsResponse);
        return statsResponse;
    }

    private Funnel getApplicableFunnel(ActionRequest actionRequest) {
        Funnel applicableFunnel = funnelStore.getByFunnelId(FunnelExtrapolationUtils.ensureFunnelId(actionRequest)
                .toString());
        Preconditions.checkNotNull(applicableFunnel, "Funnel not found for extrapolation");
        return applicableFunnel;
    }

    public ActionResponse visit(StatsTrendResponse statsTrendResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, statsTrendResponse);
        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        StatsTrendRequest statsTrendRequest = (StatsTrendRequest) actionRequest;

        List<HistogramValue> extrapolationFactors = computeExtrapolationFactors(statsTrendRequest.getTable(),
                statsTrendRequest.getTimestamp(), statsTrendRequest.getPeriod(), applicableFunnel);

        List<StatsTrendValue> statsTrendValues = statsTrendResponse.getResult();
        extrapolateStatsTrendValues(extrapolationFactors, statsTrendValues);

        List<BucketResponse<List<StatsTrendValue>>> buckets = statsTrendResponse.getBuckets();
        extrapolateStatsTrendBuckets(extrapolationFactors, buckets);
        log.debug(EXTRAPOLATED_QUERY_RESPONSE, statsTrendResponse);
        return statsTrendResponse;
    }

    public ActionResponse visit(TrendResponse trendResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, trendResponse);
        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        TrendRequest trendRequest = (TrendRequest) actionRequest;

        List<HistogramValue> extrapolationFactors = computeExtrapolationFactors(trendRequest.getTable(),
                trendRequest.getTimestamp(), trendRequest.getPeriod(), applicableFunnel);

        if (CollectionUtils.isNotEmpty(trendResponse.getTrends())) {
            trendResponse.getTrends()
                    .values()
                    .forEach(counts -> {
                        if (counts.size() == extrapolationFactors.size()) {
                            for (int i = 0; i < counts.size(); i++) {
                                long originalCount = counts.get(i)
                                        .getCount();
                                counts.get(i)
                                        .setCount((long) (originalCount * extrapolationFactors.get(i)
                                                .getValue()));
                            }
                        }
                    });
        }
        log.debug(EXTRAPOLATED_QUERY_RESPONSE, trendResponse);
        return trendResponse;
    }

    public ActionResponse visit(CountResponse countResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, countResponse);

        Funnel applicableFunnel = getApplicableFunnel(actionRequest);

        double extrapolationFactor = computeExtrapolationFactor(applicableFunnel);
        countResponse.setCount((long) (countResponse.getCount() * extrapolationFactor));

        log.debug(EXTRAPOLATED_QUERY_RESPONSE, countResponse);
        return countResponse;
    }

    // Extrapolation not applicable
    public ActionResponse visit(DistinctResponse distinctResponse) {
        return distinctResponse;
    }

    public ActionResponse visit(MultiQueryResponse multiQueryResponse) {
        log.debug(ORIGINAL_QUERY_RESPONSE, multiQueryResponse);

        MultiQueryRequest multiQueryRequest = (MultiQueryRequest) actionRequest;
        if (CollectionUtils.isNotEmpty(multiQueryResponse.getResponses())) {
            for (Map.Entry<String, ActionResponse> entry : multiQueryResponse.getResponses()
                    .entrySet()) {
                FunnelExtrapolationResponseVisitor funnelExtrapolationResponseVisitor = new FunnelExtrapolationResponseVisitor(
                        multiQueryRequest.getRequests()
                                .get(entry.getKey()), queryExecutor, funnelStore, funnelConfiguration);
                entry.setValue(entry.getValue()
                        .accept(funnelExtrapolationResponseVisitor));
            }
        }
        log.debug(EXTRAPOLATED_QUERY_RESPONSE, multiQueryResponse);
        return multiQueryResponse;
    }

    // TODO extrapolate multi time query response
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


    private void extrapolateStatsTrendBuckets(List<HistogramValue> extrapolationFactors,
                                              List<BucketResponse<List<StatsTrendValue>>> buckets) {
        if (CollectionUtils.isNotEmpty(buckets)) {
            for (BucketResponse<List<StatsTrendValue>> bucketResponse : buckets) {
                extrapolateStatsTrendValues(extrapolationFactors, bucketResponse.getResult());
                extrapolateStatsTrendBuckets(extrapolationFactors, bucketResponse.getBuckets());
            }
        }
    }

    private void extrapolateStatsTrendValues(List<HistogramValue> extrapolationFactors,
                                             List<StatsTrendValue> statsTrendValues) {
        if (CollectionUtils.isNotEmpty(statsTrendValues) && extrapolationFactors.size() == statsTrendValues.size()) {
            for (int i = 0; i < statsTrendValues.size(); i++) {
                Map<String, Number> originalStats = statsTrendValues.get(i)
                        .getStats();
                Map<String, Number> extrapolatedStats = extrapolateStats(extrapolationFactors.get(i)
                        .getValue(), originalStats);
                statsTrendValues.get(i)
                        .setStats(extrapolatedStats);
            }
        }
    }

    private double computeExtrapolationFactor(Funnel applicableFunnel) {
        String table = actionRequest.accept(tableActionRequestVisitor);
        long totalBaseEventCount = getTotalBaseEventCount(table, applicableFunnel);

        long baseEventCountForFunnelId = getBaseEventCountForFunnelId(applicableFunnel, table);

        if (baseEventCountForFunnelId != 0) {
            double extrapolationFactor = FunnelExtrapolationResponseVisitor.this.computeExtrapolationFactor(
                    applicableFunnel, totalBaseEventCount, baseEventCountForFunnelId);
            log.info("Total Base Event Count: {}, Base Event Count for funnel id:{} is {},"
                            + " funnelPercentage:{}, calculated extrapolation factor: {}", totalBaseEventCount,
                    applicableFunnel.getId(), baseEventCountForFunnelId, applicableFunnel.getPercentage(),
                    extrapolationFactor);
            return extrapolationFactor;
        }
        return 1.0;
    }

    private double computeExtrapolationFactor(Funnel applicableFunnel,
                                              double totalBaseEventCount,
                                              long baseEventCountForFunnelId) {
        return (totalBaseEventCount / baseEventCountForFunnelId) * ((double) 100 / applicableFunnel.getPercentage());
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

    private long getBaseEventCountForFunnelId(Funnel applicableFunnel,
                                              String table) {
        CountRequest countRequest = buildBaseEventCountRequest(table, applicableFunnel);

        countRequest.getFilters()
                .add(funnelIdFilter(applicableFunnel));

        log.debug("Base Event Count with funnel id : {}, request : {}", applicableFunnel.getId(), countRequest);
        return ((CountResponse) queryExecutor.execute(countRequest)).getCount();
    }

    private GreaterEqualFilter funnelIdFilter(Funnel applicableFunnel) {
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setField(FUNNEL_ID_QUERY_FIELD);
        greaterEqualFilter.setTemporal(false);
        greaterEqualFilter.setValue(Long.valueOf(applicableFunnel.getId()));
        return greaterEqualFilter;
    }

    private long getTotalBaseEventCount(String table,
                                        Funnel applicableFunnel) {
        CountRequest totalCountRequest = buildBaseEventCountRequest(table, applicableFunnel);

        log.debug("Total Base Event Count request : {}", totalCountRequest);
        return ((CountResponse) queryExecutor.execute(totalCountRequest)).getCount();
    }

    private CountRequest buildBaseEventCountRequest(String table,
                                                    Funnel applicableFunnel) {
        CountRequest countRequest = CountRequest.builder()
                .table(table)
                .build();

        List<Filter> filters = baseEventFilters(applicableFunnel);

        countRequest.setFilters(filters);

        return countRequest;
    }

    private List<Filter> baseEventFilters(Funnel applicableFunnel) {
        List<Filter> filters = new ArrayList<>();

        // Add eventType filter
        EqualsFilter eventTypeFilter = new EqualsFilter(EVENT_TYPE, funnelConfiguration.getBaseFunnelEventConfig()
                .getEventType());
        filters.add(eventTypeFilter);

        // Add category filter
        EqualsFilter categoryFilter = new EqualsFilter(CATEGORY, funnelConfiguration.getBaseFunnelEventConfig()
                .getCategory());
        filters.add(categoryFilter);

        // Add versionCode filter
        filters.add(versionCodeFilter());

        filters.addAll(temporalFilter(applicableFunnel));
        return filters;
    }

    private List<Filter> temporalFilter(Funnel applicableFunnel) {
        List<Filter> temporalFilters = actionRequest.getFilters()
                .stream()
                .filter(Filter::isFilterTemporal)
                .collect(Collectors.toList());

        temporalFilters.add(funnelApprovedTimeFilter(applicableFunnel));

        List<Field> dateFields = Arrays.asList(Date.class.getDeclaredFields());
        List<String> dateFieldNames = dateFields.stream()
                .map(field -> DATE + DOT + field.getName())
                .collect(Collectors.toList());
        temporalFilters.addAll(actionRequest.getFilters()
                .stream()
                .filter(filter -> dateFieldNames.contains(filter.getField()))
                .collect(Collectors.toList()));
        return temporalFilters;
    }

    private Filter versionCodeFilter() {
        return actionRequest.getFilters()
                .stream()
                .filter(filter -> filter.getField()
                        .equals(funnelConfiguration.getDefaultVersionCodeFilter()
                                .getField()))
                .findFirst()
                .orElse(funnelConfiguration.getDefaultVersionCodeFilter());
    }

    private Filter funnelApprovedTimeFilter(Funnel applicableFunnel) {
        GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
        greaterEqualFilter.setTemporal(true);
        greaterEqualFilter.setField(TIMESTAMP);
        greaterEqualFilter.setValue(applicableFunnel.getApprovedAt()
                .getTime());
        return greaterEqualFilter;
    }

    private List<HistogramValue> computeExtrapolationFactors(String table,
                                                             String field,
                                                             Period period,
                                                             Funnel applicableFunnel) {
        List<HistogramValue> extrapolationFactors = new ArrayList<>();
        List<Count> totalBaseEventCounts = getTotalBaseEventCount(table, field, period, applicableFunnel).getCounts();
        List<Count> baseEventCountsForFunnelId = getBaseEventCountForFunnelId(table, field, period,
                applicableFunnel).getCounts();

        if (totalBaseEventCounts.size() != baseEventCountsForFunnelId.size()) {
            log.error("Counts different for total base event count and base event with "
                    + "funnel id count for actionRequest :{}", actionRequest);
            return extrapolationFactors;
        }

        // Assuming order is preserved in totalBaseEventCounts histogram response and
        // baseEventCountsForFunnelId histogram response we can take ratio at same indices
        for (int i = 0; i < totalBaseEventCounts.size(); i++) {
            if (baseEventCountsForFunnelId.get(i)
                    .getCount() != 0) {
                double extrapolationFactor = computeExtrapolationFactor(applicableFunnel, totalBaseEventCounts.get(i)
                        .getCount(), baseEventCountsForFunnelId.get(i)
                        .getCount());
                extrapolationFactors.add(new HistogramValue(totalBaseEventCounts.get(i)
                        .getPeriod(), extrapolationFactor));
            } else {
                extrapolationFactors.add(new HistogramValue(totalBaseEventCounts.get(i)
                        .getPeriod(), 1.0));
            }
        }
        log.info("Calculated extrapolation factors: {} for actionRequest:{}", extrapolationFactors, actionRequest);
        return extrapolationFactors;
    }

    private HistogramResponse getBaseEventCountForFunnelId(String table,
                                                           String field,
                                                           Period period,
                                                           Funnel applicableFunnel) {
        List<Filter> filters = baseEventFilters(applicableFunnel);
        filters.add(funnelIdFilter(applicableFunnel));

        HistogramRequest histogramRequest = new HistogramRequest(filters, table, field, null, period, null, false, actionRequest.getRequestTags(), actionRequest.getSourceType(), false);

        return (HistogramResponse) queryExecutor.execute(histogramRequest);
    }

    private HistogramResponse getTotalBaseEventCount(String table,
                                                     String field,
                                                     Period period,
                                                     Funnel applicableFunnel) {
        List<Filter> filters = baseEventFilters(applicableFunnel);

        HistogramRequest histogramRequest = new HistogramRequest(filters, table, field, null, period, null, false, actionRequest.getRequestTags(), actionRequest.getSourceType(), false);

        return (HistogramResponse) queryExecutor.execute(histogramRequest);
    }


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class HistogramValue {

        private Number period;
        private double value;
    }
}
