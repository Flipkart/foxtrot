package com.foxtrot.flipkart.translator.config;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class SegregationConfiguration {

    private List<TableSegregationConfig> tableSegregationConfigs = Lists.newArrayList();

}
