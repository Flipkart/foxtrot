/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:28 AM
 */
public class ElasticsearchConfig {
    public static final long DEFAULT_TIMEOUT = 10000L;
    @Valid
    @NotNull
    @JsonProperty
    private Vector<String> hosts;
    @Valid
    @NotNull
    @JsonProperty
    private String cluster;
    private String tableNamePrefix = "fo/**/xtrot";
    private long getQueryTimeout;
    private Integer port;

    public ElasticsearchConfig() {
    }

    public Vector<String> getHosts() {
        return hosts;
    }

    public void setHosts(String hostString) {
        if(hostString == null || hostString.trim()
                .isEmpty()) {
            return;
        }

        String[] hostParts = hostString.split(",");
        this.hosts = new Vector<>();
        Collections.addAll(this.hosts, hostParts);
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getTableNamePrefix() {
        return tableNamePrefix;
    }

    public void setTableNamePrefix(String tableNamePrefix) {
        this.tableNamePrefix = tableNamePrefix;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public long getGetQueryTimeout() {
        return getQueryTimeout > 0 ? getQueryTimeout : DEFAULT_TIMEOUT;
    }
}
