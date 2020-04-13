package com.foxtrot.flipkart.translator.config;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.Data;

/***
 Created by nitish.goyal on 28/08/19
 ***/
@Data
public class TableSegregationConfig {

    private String oldTable;

    private Map<String, List<String>> newTableVsEventTypes = Maps.newHashMap();

}
