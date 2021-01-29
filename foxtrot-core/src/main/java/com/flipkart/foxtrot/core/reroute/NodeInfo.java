package com.flipkart.foxtrot.core.reroute;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeInfo {

    private List<ShardInfo> shardInfos;
}
