package com.flipkart.foxtrot.core.lock;

import lombok.Data;

@Data
public class DistributedLockGroupConfig {

    private long lockExpiryTimeInMs = 500;
}
