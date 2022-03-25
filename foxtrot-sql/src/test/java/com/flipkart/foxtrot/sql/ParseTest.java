package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.exception.FqlParsingException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.ResultSort.Order;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.MissingFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.flipkart.foxtrot.sql.query.FqlDescribeTable;
import com.flipkart.foxtrot.sql.query.FqlShowTablesQuery;
import com.google.common.collect.ImmutableList;
import io.dropwizard.util.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class ParseTest {

    private ObjectMapper objectMapper;
    private ObjectWriter writer;

    @Before
    public void init() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        writer = objectMapper.writerWithDefaultPrettyPrinter();
    }

    @Test
    public void testWhereCommand() throws Exception {
        String sql = "select * from europa where a is null";
        QueryTranslator queryTranslator = new QueryTranslator();
        Query query = new Query();
        query.setTable("europa");
        query.setFrom(0);
        query.setLimit(10);
        query.setSort(null);
        query.setFilters(ImmutableList.of(new MissingFilter("a")));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testLimitOffsetCommand() throws JsonProcessingException {
        String sql = "select abc.xyz, def from europa order by test.name limit 20 offset 5";
        QueryTranslator queryTranslator = new QueryTranslator();
        Query query = new Query();
        query.setTable("europa");
        query.setFrom(5);
        query.setLimit(20);
        ResultSort sort = new ResultSort("test.name", Order.asc);
        query.setSort(sort);
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, ImmutableList.of("abc.xyz", "def"));
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testGroupByCommand() throws JsonProcessingException {
        String sql = "select * from europa group by test.name, test.surname";
        QueryTranslator queryTranslator = new QueryTranslator();
        GroupRequest query = new GroupRequest();
        query.setTable("europa");
        query.setNesting(ImmutableList.of("test.name", "test.surname"));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testMixed() throws JsonProcessingException {
        String sql = "select * from europa where a = 'b' and c = 2.5 and temporal(e) between 10 and 30 and x > 99 order by test.name limit 20 offset 5";
        QueryTranslator queryTranslator = new QueryTranslator();
        Query query = new Query();
        query.setTable("europa");
        query.setFrom(5);
        query.setLimit(20);
        ResultSort sort = new ResultSort("test.name", Order.asc);
        query.setSort(sort);
        ImmutableList filters = ImmutableList.of(new EqualsFilter("a", "b"), new EqualsFilter("c", 2.5),
                new BetweenFilter("e", 10, 30, true), new GreaterThanFilter("x", 99, false));
        query.setFilters(filters);
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testTrendCommand() throws JsonProcessingException {
        String sql = "select trend(header.configName, 'minutes', 'header.timestamp') from europa where a != 10 and a <= 20";
        QueryTranslator queryTranslator = new QueryTranslator();
        TrendRequest query = new TrendRequest();
        query.setTable("europa");
        query.setField("header.configName");
        query.setPeriod(Period.minutes);
        query.setTimestamp("header.timestamp");
        query.setFilters(ImmutableList.of(new NotEqualsFilter("a", 10), new LessEqualFilter("a", 20, false)));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testStatsTrendCommand() throws JsonProcessingException {
        String sql = "select statstrend(header.configName, 'minutes') from europa where a < 10";
        QueryTranslator queryTranslator = new QueryTranslator();
        StatsTrendRequest query = new StatsTrendRequest();
        query.setTable("europa");
        query.setField("header.configName");
        query.setPeriod(Period.minutes);
        query.setFilters(ImmutableList.of(new LessThanFilter("a", 10, false)));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testStatsCommand() throws JsonProcessingException {
        String sql = "select stats(header.configName) from europa where a >=10";
        QueryTranslator queryTranslator = new QueryTranslator();
        StatsRequest query = new StatsRequest();
        query.setTable("europa");
        query.setField("header.configName");
        query.setFilters(ImmutableList.of(new GreaterEqualFilter("a", 10, false)));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testHistogramCommand() throws JsonProcessingException {
        String sql = "select histogram('minutes', 'header.timestamp') from europa where a in ('b', 'c')";
        QueryTranslator queryTranslator = new QueryTranslator();
        HistogramRequest query = new HistogramRequest();
        query.setTable("europa");
        query.setField("header.timestamp");
        query.setPeriod(Period.minutes);
        query.setFilters(ImmutableList.of(new InFilter("a", ImmutableList.of("b", "c"))));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testCountCommand() throws JsonProcessingException {
        String sql = "select count('header.timestamp') from europa where a like 'b'";
        QueryTranslator queryTranslator = new QueryTranslator();
        CountRequest query = new CountRequest();
        query.setTable("europa");
        query.setField("header.timestamp");
        ContainsFilter filter = new ContainsFilter("b");
        filter.setField("a");
        query.setFilters(ImmutableList.of(filter));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testTimeRangeCommand() throws JsonProcessingException {
        String sql = "select eventType from europa where last('1m', 1000)";
        QueryTranslator queryTranslator = new QueryTranslator();
        Query query = new Query();
        query.setTable("europa");
        LastFilter filter = new LastFilter();
        filter.setDuration(Duration.minutes(1));
        filter.setCurrentTime(1000);
        query.setFilters(ImmutableList.of(filter));
        query.setSort(null);
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, ImmutableList.of("eventType"));
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test
    public void testDistinctCommand() throws JsonProcessingException {
        String sql = "select distinct(eventType) from europa";
        QueryTranslator queryTranslator = new QueryTranslator();
        DistinctRequest query = new DistinctRequest();
        query.setTable("europa");
        query.setNesting(ImmutableList.of(new ResultSort("eventType", Order.desc)));
        FqlActionQuery fqlActionQuery = new FqlActionQuery(query, ImmutableList.of("eventType"));
        Assert.assertEquals(writer.writeValueAsString(fqlActionQuery),
                writer.writeValueAsString(queryTranslator.translate(sql)));
    }

    @Test(expected = FqlParsingException.class)
    public void testWrongFqlParseFail() throws JsonProcessingException {
        String sql = "select distinct(eventType) from europa order by a desc where a = 10";
        QueryTranslator queryTranslator = new QueryTranslator();
        queryTranslator.translate(sql);
    }

    @Test
    public void testShowTablesCommand() {
        String sql = "show tables";
        QueryTranslator queryTranslator = new QueryTranslator();
        FqlShowTablesQuery query = (FqlShowTablesQuery) queryTranslator.translate(sql);
        Assert.assertNotNull(query);
    }

    @Test
    public void testDescribeCommandTable() {
        String sql = "desc europa";
        QueryTranslator queryTranslator = new QueryTranslator();
        FqlQuery query = queryTranslator.translate(sql);
        Assert.assertNotNull(query);
        Assert.assertEquals("europa", ((FqlDescribeTable) query).getTableName());
    }

    @Test
    public void testGroupAggregationSumQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select sum(eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertEquals("eventData.amount", groupRequest.getAggregationField());
        Assert.assertNotNull(groupRequest.getAggregationType());
        Assert.assertEquals(Stat.SUM, groupRequest.getAggregationType());

    }

    @Test
    public void testGroupAggregationCountDistinctQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select count(distinct eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertEquals("eventData.amount", groupRequest.getUniqueCountOn());
        Assert.assertNull(groupRequest.getAggregationType());

    }

    @Test
    public void testGroupAggregationCountQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select count(eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertEquals("eventData.amount", groupRequest.getAggregationField());
        Assert.assertNotNull(groupRequest.getAggregationType());
        Assert.assertNull(groupRequest.getUniqueCountOn());
        Assert.assertEquals(Stat.COUNT, groupRequest.getAggregationType());
    }

    @Test
    public void testGroupAggregationAvgQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select avg(eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertEquals("eventData.amount", groupRequest.getAggregationField());
        Assert.assertNotNull(groupRequest.getAggregationType());

        Assert.assertNull(groupRequest.getUniqueCountOn());
        Assert.assertEquals(Stat.AVG, groupRequest.getAggregationType());

    }

    @Test
    public void testGroupAggregationMinQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select min(eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertEquals("eventData.amount", groupRequest.getAggregationField());
        Assert.assertNotNull(groupRequest.getAggregationType());
        Assert.assertNull(groupRequest.getUniqueCountOn());
        Assert.assertEquals(Stat.MIN, groupRequest.getAggregationType());

    }

    @Test
    public void testGroupAggregationMaxQueryParsing() {
        QueryTranslator queryTranslator = new QueryTranslator();
        String sql = "select max(eventData.amount) from europa where eventType = 'AWESOME_EVENT' group by date.hourOfDay";
        FqlQuery fqlQuery = queryTranslator.translate(sql);
        Assert.assertTrue(fqlQuery instanceof FqlActionQuery);
        FqlActionQuery actionQuery = (FqlActionQuery) fqlQuery;
        Assert.assertTrue(actionQuery.getActionRequest() instanceof GroupRequest);
        GroupRequest groupRequest = (GroupRequest) actionQuery.getActionRequest();

        Assert.assertEquals(1, groupRequest.getNesting()
                .size());
        Assert.assertTrue(groupRequest.getNesting()
                .contains("date.hourOfDay"));
        Assert.assertNull(groupRequest.getUniqueCountOn());
        Assert.assertEquals("eventData.amount", groupRequest.getAggregationField());
        Assert.assertEquals(Stat.MAX, groupRequest.getAggregationType());

    }
}
