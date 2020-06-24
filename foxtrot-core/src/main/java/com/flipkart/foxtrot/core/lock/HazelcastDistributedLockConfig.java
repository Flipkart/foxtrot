package com.flipkart.foxtrot.core.lock;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HazelcastDistributedLockConfig {

    private Map<String, DistributedLockGroupConfig> locksConfig;
}
