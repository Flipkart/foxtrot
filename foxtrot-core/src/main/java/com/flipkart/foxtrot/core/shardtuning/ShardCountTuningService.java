package com.flipkart.foxtrot.core.shardtuning;

public interface ShardCountTuningService {

    void tuneShardCount();

    void tuneShardCount(String table);

}
