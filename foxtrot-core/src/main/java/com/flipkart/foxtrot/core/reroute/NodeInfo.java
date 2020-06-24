package com.flipkart.foxtrot.core.reroute;

import java.util.List;
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
public class NodeInfo {

    private List<ShardInfo> shardInfos;
}
