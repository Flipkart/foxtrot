/**
 * Copyright 2016 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

/**
 * AWS ECS based cluster configuration.
 * See: https://github.com/hazelcast/hazelcast-aws#ecsfargate-configuration
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AwsECSDiscoveryConfig extends ClusterDiscoveryConfig {

    @NotEmpty
    @JsonProperty
    private String network;

    @JsonProperty
    private String accessKey;

    @JsonProperty
    private String secretKey;

    @JsonProperty
    private String region;

    @JsonProperty
    private String cluster;

    @JsonProperty
    private String family;

    @JsonProperty
    private String serviceName;

    @JsonProperty
    private String hostHeader;

    @JsonProperty
    @Min(0)
    private int opTimeoutSeconds;

    @JsonProperty
    private boolean isExternalClient;

    public AwsECSDiscoveryConfig() {
        super(ClusterDiscoveryType.FOXTROT_AWS_ECS);
    }

}
