/**
 * Copyright 2016 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SimpleClusterDiscoveryConfig extends ClusterDiscoveryConfig {

    @JsonProperty
    private List<String> members = null;

    @JsonProperty
    private boolean disableMulticast = false;

    public SimpleClusterDiscoveryConfig() {
        super(ClusterDiscoveryType.foxtrot_simple);
    }

    public List<String> getMembers() {
        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }

    public boolean isDisableMulticast() {
        return disableMulticast;
    }

    public void setDisableMulticast(boolean disableMulticast) {
        this.disableMulticast = disableMulticast;
    }
}
