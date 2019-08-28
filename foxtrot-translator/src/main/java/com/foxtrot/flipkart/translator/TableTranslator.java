package com.foxtrot.flipkart.translator;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.Document;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TableSegregationConfig;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.Map;

/***
 Created by nitish.goyal on 28/08/19
 ***/
@Slf4j
public class TableTranslator {

    private static final String EVENT_TYPE = "eventType";
    private final Map<String, TableSegregationConfig> tableVsSegregationConfig = Maps.newHashMap();

    @Inject
    public TableTranslator(SegregationConfiguration segregationConfiguration) {
        if (segregationConfiguration != null){
            segregationConfiguration.getTableSegregationConfigs().forEach(tableSegregationConfig ->
                    tableVsSegregationConfig.put(tableSegregationConfig.getOldTable(), tableSegregationConfig));
        }
    }

    public String getTable(String table, Document document) {
        if (document.getData()
                .has(EVENT_TYPE)) {
            String eventType = document.getData()
                    .get(EVENT_TYPE)
                    .asText();
            return getSegregatedTableName(table, eventType);
        }
        return table;
    }

    public boolean isTransformTable(String table) {
        return tableVsSegregationConfig.get(table) != null;
    }

    private String getSegregatedTableName(String table, String eventType) {
        if (CollectionUtils.isNotEmpty(tableVsSegregationConfig) && tableVsSegregationConfig.get(table) != null) {
            TableSegregationConfig tableSegregationConfig = tableVsSegregationConfig.get(table);
            if (tableSegregationConfig.getEventTypes().contains(eventType)) {
                return tableSegregationConfig.getNewTable();
            }
        }
        return table;
    }

}
