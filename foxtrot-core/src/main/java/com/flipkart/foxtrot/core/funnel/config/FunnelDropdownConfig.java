package com.flipkart.foxtrot.core.funnel.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class FunnelDropdownConfig {

    private List<String> eventTypes = new ArrayList<>();

    private List<String> categories = new ArrayList<>();

    private List<String> deviceAttributes = new ArrayList<>();
}
