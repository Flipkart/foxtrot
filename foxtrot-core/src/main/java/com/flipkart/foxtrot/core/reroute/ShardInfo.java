package com.flipkart.foxtrot.core.reroute;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.index.shard.ShardId;

/***
 Created by mudit.g on Feb, 2019
 ***/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShardInfo {

    private ShardId shardId;

    private long shardSize;
}
