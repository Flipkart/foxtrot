package com.flipkart.foxtrot.core.lock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HazelcastDistributedLockConfig {

    private Map<String, DistributedLockGroupConfig> locksConfig;
}
