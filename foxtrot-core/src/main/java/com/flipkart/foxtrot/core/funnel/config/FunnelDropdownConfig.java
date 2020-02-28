package com.flipkart.foxtrot.core.funnel.config;

import java.util.List;
import lombok.Data;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class FunnelDropdownConfig {

    private List<String> eventIds;

    private List<String> identifierIds;

    private List<String> deviceAttributes;
}
