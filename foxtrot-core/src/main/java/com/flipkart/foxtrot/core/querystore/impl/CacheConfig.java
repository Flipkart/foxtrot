package com.flipkart.foxtrot.core.querystore.impl;

import lombok.Data;

/***
 Created by nitish.goyal on 20/09/18
 ***/
@Data
public class CacheConfig {

    private int maxIdleSeconds;
    private int timeToLiveSeconds;
    private int size;


}
