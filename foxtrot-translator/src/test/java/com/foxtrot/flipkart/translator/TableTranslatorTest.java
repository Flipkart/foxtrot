package com.foxtrot.flipkart.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TableSegregationConfig;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class TableTranslatorTest {

    private TableTranslator tableTranslator;
    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void init() {
        Map<String, List<String>> newTableVsEventTypes = Maps.newHashMap();
        newTableVsEventTypes.put("newTable", ImmutableList.of("APP_LOADED"));

        TableSegregationConfig tableSegregationConfig = new TableSegregationConfig();
        tableSegregationConfig.setOldTable("oldTable");
        tableSegregationConfig.setNewTableVsEventTypes(newTableVsEventTypes);

        List<TableSegregationConfig> tableSegregationConfigs = ImmutableList.of(tableSegregationConfig);
        SegregationConfiguration segregationConfiguration = new SegregationConfiguration();
        segregationConfiguration.setTableSegregationConfigs(tableSegregationConfigs);

        tableTranslator = new TableTranslator(segregationConfiguration);
    }

    @Test
    public void testTableTranslation() {
        Document document = new Document("id", System.currentTimeMillis(), mapper.getNodeFactory()
                .objectNode()
                .put("eventType", "APP_LOADED"));
        String tableName = tableTranslator.getTable("oldTable", document);
        Assert.assertEquals("newTable", tableName);
    }

    @Test
    public void testDocWithoutEventType() {
        Document document = new Document("id", System.currentTimeMillis(), mapper.getNodeFactory()
                .objectNode()
                .put("hello", "world"));
        String tableName = tableTranslator.getTable("oldTable", document);
        Assert.assertEquals("oldTable", tableName);
    }

    @Test
    public void testEventTypeWithNoTranslation() {
        Document document = new Document("id", System.currentTimeMillis(), mapper.getNodeFactory()
                .objectNode()
                .put("eventType", "world"));
        String tableName = tableTranslator.getTable("oldTable", document);
        Assert.assertEquals("oldTable", tableName);
    }

    @Test
    public void testTableWithNoTranslation() {
        Document document = new Document("id", System.currentTimeMillis(), mapper.getNodeFactory()
                .objectNode()
                .put("eventType", "APP_LOADED"));
        String tableName = tableTranslator.getTable("table1", document);
        Assert.assertEquals("table1", tableName);
    }
}
