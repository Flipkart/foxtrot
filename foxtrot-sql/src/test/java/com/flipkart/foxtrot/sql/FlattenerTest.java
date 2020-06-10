package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.QueryResponse;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.exception.FqlParsingException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse.Count;
import com.flipkart.foxtrot.common.query.MultiQueryRequest;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryRequest;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.sql.responseprocessors.Flattener;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Test;

public class FlattenerTest {

    private static final String TABLE_NAME = "tableName";
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void flattenQueryResponseTest() throws IOException {
        Query query = new Query();
        query.setTable(TABLE_NAME);
        List<String> fields = ImmutableList.of("name", "role");
        Flattener flattener = new Flattener(objectMapper, query, fields);

        QueryResponse response = new QueryResponse();
        final String json1 = "{\"name\": \"abc\", \"role\": \"def\"}";
        final String json2 = "{\"name\": \"xyz\", \"role\": \"zab\"}";
        response.setDocuments(ImmutableList.of(Document.builder()
                .data(objectMapper.readTree(json1))
                .build(), Document.builder()
                .data(objectMapper.readTree(json2))
                .build()));

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(2, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals(fields.get(0), flatRepresentation.getHeaders()
                .get(0)
                .getName());
        Assert.assertEquals(fields.get(1), flatRepresentation.getHeaders()
                .get(1)
                .getName());

        Map<String, String> row1 = new TreeMap<>();
        row1.put(fields.get(0), "\"abc\"");
        row1.put(fields.get(1), "\"def\"");
        Map<String, String> row2 = new TreeMap<>();
        row2.put(fields.get(0), "\"xyz\"");
        row2.put(fields.get(1), "\"zab\"");
        Assert.assertEquals(2, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(row1.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
        Assert.assertEquals(row2.toString(), flatRepresentation.getRows()
                .get(1)
                .toString());
    }

    @Test
    public void flattenGroupResponseTest() {
        final String groupingField = "eventType";

        GroupRequest query = new GroupRequest();
        query.setTable(TABLE_NAME);
        query.setNesting(ImmutableList.of(groupingField));
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());

        Map<String, Object> responseResult = new TreeMap<>();
        responseResult.put("type1", 20);
        responseResult.put("type2", 30);
        responseResult.put("type3", 40);
        GroupResponse response = new GroupResponse(responseResult);

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(2, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals(groupingField, flatRepresentation.getHeaders()
                .get(0)
                .getName());
        Assert.assertEquals("count", flatRepresentation.getHeaders()
                .get(1)
                .getName());

        Map<String, String> type1 = new TreeMap<>();
        type1.put(groupingField, "type1");
        type1.put("count", "20");
        Map<String, String> type2 = new TreeMap<>();
        type2.put(groupingField, "type2");
        type2.put("count", "30");
        Map<String, String> type3 = new TreeMap<>();
        type3.put(groupingField, "type3");
        type3.put("count", "40");
        Assert.assertEquals(3, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(type1.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
        Assert.assertEquals(type2.toString(), flatRepresentation.getRows()
                .get(1)
                .toString());
        Assert.assertEquals(type3.toString(), flatRepresentation.getRows()
                .get(2)
                .toString());
    }

    @Test
    public void flattenHistogramResponseTest() {
        HistogramRequest query = new HistogramRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        HistogramResponse response = new HistogramResponse(ImmutableList.of(new Count(1234, 12), new Count(1235, 10)));

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(2, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("timestamp", flatRepresentation.getHeaders()
                .get(0)
                .getName());
        Assert.assertEquals("count", flatRepresentation.getHeaders()
                .get(1)
                .getName());

        Map<String, String> bin1 = new TreeMap<>();
        bin1.put("timestamp", "1234");
        bin1.put("count", "12");
        Map<String, String> bin2 = new TreeMap<>();
        bin2.put("timestamp", "1235");
        bin2.put("count", "10");
        Assert.assertEquals(2, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(bin1.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
        Assert.assertEquals(bin2.toString(), flatRepresentation.getRows()
                .get(1)
                .toString());
    }

    @Test
    public void flattenStatsResponseTest() {
        StatsRequest query = new StatsRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());

        StatsValue statsValue = new StatsValue();
        Map<Number, Number> percentiles = new HashMap<>();
        percentiles.put(95, 2);
        statsValue.setPercentiles(percentiles);
        Map<String, Number> stats = new HashMap<>();
        stats.put("stat1", 2);
        statsValue.setStats(stats);
        StatsResponse response = new StatsResponse(statsValue);

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(15, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("stats.count", flatRepresentation.getHeaders()
                .get(7)
                .getName());
        Assert.assertEquals("percentiles.50.0", flatRepresentation.getHeaders()
                .get(3)
                .getName());
        Assert.assertEquals("stats.sum_of_squares", flatRepresentation.getHeaders()
                .get(12)
                .getName());

        Map<String, String> stat = new TreeMap<>();
        stat.put("percentiles.95", "2");
        stat.put("stats.stat1", "2");
        Assert.assertEquals(1, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(stat.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
    }

    @Test
    public void flattenTrendResponseTest() {
        TrendRequest query = new TrendRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        Map<String, List<TrendResponse.Count>> trends = new HashMap<>();
        trends.put("trend1", ImmutableList.of(new TrendResponse.Count(1234, 10), new TrendResponse.Count(1235, 12)));
        trends.put("trend2", ImmutableList.of(new TrendResponse.Count(1234, 20), new TrendResponse.Count(1235, 22)));
        TrendResponse response = new TrendResponse(trends);

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(3, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("time", flatRepresentation.getHeaders()
                .get(0)
                .getName());
        Assert.assertEquals("trend1", flatRepresentation.getHeaders()
                .get(1)
                .getName());
        Assert.assertEquals("trend2", flatRepresentation.getHeaders()
                .get(2)
                .getName());

        Map<String, String> trend1 = new TreeMap<>();
        trend1.put("time", "1234");
        trend1.put("trend1", "10");
        trend1.put("trend2", "20");
        Map<String, String> trend2 = new TreeMap<>();
        trend2.put("time", "1235");
        trend2.put("trend1", "12");
        trend2.put("trend2", "22");
        Assert.assertEquals(2, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(trend1.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
        Assert.assertEquals(trend2.toString(), flatRepresentation.getRows()
                .get(1)
                .toString());
    }

    @Test
    public void flattenStatsTrendResponseTest() {
        StatsTrendRequest query = new StatsTrendRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        StatsTrendResponse response = new StatsTrendResponse();

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(16, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("stats.count", flatRepresentation.getHeaders()
                .get(8)
                .getName());
        Assert.assertEquals("percentiles.50.0", flatRepresentation.getHeaders()
                .get(4)
                .getName());
        Assert.assertEquals("stats.sum_of_squares", flatRepresentation.getHeaders()
                .get(13)
                .getName());
        Assert.assertEquals(0, flatRepresentation.getRows()
                .size());
    }

    @Test
    public void flattenCountResponseTest() {
        CountRequest query = new CountRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        CountResponse response = new CountResponse(10);

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(1, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("count", flatRepresentation.getHeaders()
                .get(0)
                .getName());

        Map<String, String> row = new TreeMap<>();
        row.put("count", "10");
        Assert.assertEquals(1, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(row.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
    }

    @Test
    public void flattenDistinctResponseTest() {
        DistinctRequest query = new DistinctRequest();
        query.setTable(TABLE_NAME);
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());

        DistinctResponse response = new DistinctResponse();
        response.setHeaders(ImmutableList.of("st1"));
        response.setResult(ImmutableList.of(ImmutableList.of("val11"), ImmutableList.of("val12")));

        FlatRepresentation flatRepresentation = response.accept(flattener);
        Assert.assertEquals(1, flatRepresentation.getHeaders()
                .size());
        Assert.assertEquals("st1", flatRepresentation.getHeaders()
                .get(0)
                .getName());

        Map<String, String> val1 = new TreeMap<>();
        val1.put("st1", "val11");
        Map<String, String> val2 = new TreeMap<>();
        val2.put("st1", "val12");
        Assert.assertEquals(2, flatRepresentation.getRows()
                .size());
        Assert.assertEquals(val1.toString(), flatRepresentation.getRows()
                .get(0)
                .toString());
        Assert.assertEquals(val2.toString(), flatRepresentation.getRows()
                .get(1)
                .toString());
    }

    @Test(expected = FqlParsingException.class)
    public void flattenMultiQueryResponseTest() {
        MultiQueryRequest query = new MultiQueryRequest();
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        MultiQueryResponse response = new MultiQueryResponse();
        FlatRepresentation result = response.accept(flattener);
    }

    @Test(expected = FqlParsingException.class)
    public void flattenMultiTimeQueryResponseTest() {
        MultiTimeQueryRequest query = new MultiTimeQueryRequest();
        Flattener flattener = new Flattener(objectMapper, query, ImmutableList.of());
        MultiTimeQueryResponse response = new MultiTimeQueryResponse();
        FlatRepresentation result = response.accept(flattener);
    }
}
