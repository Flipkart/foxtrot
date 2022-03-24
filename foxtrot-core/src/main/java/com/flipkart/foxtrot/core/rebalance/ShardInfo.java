package com.flipkart.foxtrot.core.rebalance;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardInfo {

    private String shard;
    private String index;
}
