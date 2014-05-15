package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Vector;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:28 AM
 */
public class ElasticsearchConfig {
    @Valid
    @NotNull
    @JsonProperty
    private Vector<String> hosts;
    @Valid
    @NotNull
    @JsonProperty
    private String cluster;

    public ElasticsearchConfig() {
    }

    public Vector<String> getHosts() {
        return hosts;
    }

    public void setHosts(Vector<String> hosts) {
        this.hosts = hosts;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
