package com.foxtrot.flipkart.translator.config;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.Data;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class SegregationConfiguration {

    private List<TableSegregationConfig> tableSegregationConfigs = Lists.newArrayList();

}
