package com.flipkart.foxtrot.core.funnel.config;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class FunnelDropdownConfig {

    private List<String> eventTypes = new ArrayList<>();

    private List<String> categories = new ArrayList<>();

    private List<String> deviceAttributes = new ArrayList<>();
}
