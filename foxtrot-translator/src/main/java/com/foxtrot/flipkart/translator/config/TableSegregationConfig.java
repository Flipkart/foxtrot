package com.foxtrot.flipkart.translator.config;

import lombok.Data;

import java.util.List;

/***
 Created by nitish.goyal on 28/08/19
 ***/
@Data
public class TableSegregationConfig {

    private String oldTable;
    private String newTable;

    private List<String> eventTypes;

}
