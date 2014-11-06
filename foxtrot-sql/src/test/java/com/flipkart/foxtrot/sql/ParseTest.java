package com.flipkart.foxtrot.sql;

import org.junit.Test;

public class ParseTest {
    @Test
    public void test() {
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
    }
}
