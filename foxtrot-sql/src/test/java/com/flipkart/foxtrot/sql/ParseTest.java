package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.general.MissingFilter;
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
            String sql = "select * from europa where a = 'b' and c=2.5 and temporal(e) between 10 and 30 and x > 99 order by test.name limit 20 offset 5";
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
            FqlActionQuery fqlActionQuery = new FqlActionQuery(FqlQueryType.select,query, new ArrayList<String>());
            Assert.assertEquals(writer.writeValueAsString(fqlActionQuery), writer.writeValueAsString(queryTranslator.translate(sql)));

        }
    }
}
