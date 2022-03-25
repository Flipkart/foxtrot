package com.flipkart.foxtrot.common.visitor;

import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.GeoAggregationRequest;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ActionRequestFilterVisitor implements ActionRequestVisitor<Map<String, List<Filter>>> {

    @Override
    public Map<String, List<Filter>> visit(GroupRequest groupRequest) {
        return ImmutableMap.of(groupRequest.getTable(), groupRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(Query query) {
        return ImmutableMap.of(query.getTable(), query.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(StatsRequest statsRequest) {
        return ImmutableMap.of(statsRequest.getTable(), statsRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(StatsTrendRequest statsTrendRequest) {
        return ImmutableMap.of(statsTrendRequest.getTable(), statsTrendRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(TrendRequest trendRequest) {
        return ImmutableMap.of(trendRequest.getTable(), trendRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(DistinctRequest distinctRequest) {
        return ImmutableMap.of(distinctRequest.getTable(), distinctRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(HistogramRequest histogramRequest) {
        return ImmutableMap.of(histogramRequest.getTable(), histogramRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(MultiTimeQueryRequest multiTimeQueryRequest) {
        return multiTimeQueryRequest.getActionRequest()
                .accept(new ActionRequestFilterVisitor());
    }

    @Override
    public Map<String, List<Filter>> visit(MultiQueryRequest multiQueryRequest) {
        List<Map<String, List<Filter>>> maps = multiQueryRequest.getRequests()
                .values()
                .stream()
                .map(actionRequest -> actionRequest.accept(new ActionRequestFilterVisitor()))
                .collect(Collectors.toList());

        Map<String, List<Filter>> result = new HashMap<>();

        maps.forEach(map -> map.forEach((key, value) -> {
            if (result.containsKey(key)) {
                result.get(key)
                        .addAll(value);
            } else {
                result.put(key, value);
            }
        }));
        return result;
    }

    @Override
    public Map<String, List<Filter>> visit(CountRequest countRequest) {
        return ImmutableMap.of(countRequest.getTable(), countRequest.getFilters());
    }

    @Override
    public Map<String, List<Filter>> visit(GeoAggregationRequest request) {
        return ImmutableMap.of(request.getTable(), request.getFilters());
    }
}
