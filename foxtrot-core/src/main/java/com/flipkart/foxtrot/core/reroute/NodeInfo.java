package com.flipkart.foxtrot.core.reroute;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeInfo {

    private long nodeSize;

    private List<ShardInfo> shardInfos;
}
