/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
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
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/09/13
 * Time: 2:12 PM
 */
public class ClusterConfig {
    @JsonProperty("name")
    @Valid
    @NotNull
    @NotEmpty
    private String name = null;

//    @JsonProperty("server-url")
//    @NotNull
//    @NotEmpty
//    private String webServerUrl = null;

    @JsonProperty
    private boolean disableMulticast = false;

    @JsonProperty
    private List<String> members = null;

    public ClusterConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

//    public String getWebServerUrl() {
//        return webServerUrl;
//    }
//
//    public void setWebServerUrl(String webServerUrl) {
//        this.webServerUrl = webServerUrl;
//    }

    public boolean isDisableMulticast() {
        return disableMulticast;
    }

    public void setDisableMulticast(boolean disableMulticast) {
        this.disableMulticast = disableMulticast;
    }

    public List<String> getMembers() {
        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }
}
