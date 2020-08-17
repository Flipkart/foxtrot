package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.general.MissingFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class ParseTest {
    @Test
    public void test() throws Exception {
        //TODO
        /*ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        {
            String sql = "select abc.xyz, def from europa order by test.name limit 20 offset 5";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            String sql = "select * from europa order by test.name limit 20 offset 5";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            String sql = "select * from europa group by test.name, test.surname";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            //String sql = "select trend(header.configName) from europa";
            //String sql = "select trend(header.configName, 'minutes') from europa";
            String sql = "select trend(header.configName, 'minutes', 'header.timestamp') from europa";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            //String sql = "select statstrend(header.configName) from europa";
            String sql = "select statstrend(header.configName, 'minutes') from europa";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            //String sql = "select statstrend(header.configName) from europa";
            String sql = "select stats(header.configName) from europa";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            //String sql = "select statstrend(header.configName) from europa";
            //String sql = "select histogram() from europa";
            //String sql = "select histogram('hours') from europa";
            String sql = "select histogram('minutes', 'header.timestamp') from europa";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }
        {
            String sql = "select * from europa where a = 'b' and c=2.5 and temporal(e) between 10 and 30 and x > 99
            order by test.name limit 20 offset 5";
            QueryTranslator queryTranslator = new QueryTranslator();
            ActionRequest request = queryTranslator.translate(sql);
            System.out.println(writer.writeValueAsString(request));
        }*/


        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        {
            String sql = "select * from europa where a is null";
            QueryTranslator queryTranslator = new QueryTranslator();
            //FqlQuery query = queryTranslator.translate(sql);
            Query query = new Query();
            query.setTable("europa");
            query.setFrom(0);
            query.setLimit(10);
            query.setSort(null);
            ImmutableList filters = ImmutableList.of(new MissingFilter("a"));
            query.setFilters(filters);
            FqlActionQuery fqlActionQuery = new FqlActionQuery(query, new ArrayList<>());
            Assert.assertEquals(writer.writeValueAsString(fqlActionQuery), writer.writeValueAsString(queryTranslator.translate(sql)));

        }
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
        Assert.assertEquals(groupRequest.getAggregationType(),Stat.SUM);

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
        Assert.assertEquals(groupRequest.getAggregationType(),Stat.COUNT);
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
        Assert.assertEquals(groupRequest.getAggregationType(),Stat.AVG);

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
        Assert.assertEquals(groupRequest.getAggregationType(),Stat.MIN);

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
        Assert.assertEquals(groupRequest.getAggregationType(),Stat.MAX);

    }
}
