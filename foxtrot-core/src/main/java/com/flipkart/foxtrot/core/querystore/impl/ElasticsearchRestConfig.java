package com.flipkart.foxtrot.core.querystore.impl;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.NoArgsConstructor;

/***
 User : nitish.goyal
 Date : 10/06/20
 Time : 4:45 PM
 ***/
@NoArgsConstructor
public class ElasticsearchRestConfig {

    public static final long DEFAULT_TIMEOUT = 10000L;
    @Valid
    @NotNull
    @JsonProperty
    private List<String> hosts;

    @Valid
    @NotNull
    @JsonProperty
    private String cluster;
    private String tableNamePrefix = "foxtrot";
    private long getQueryTimeout;
    private Integer port;
    @NotNull
    @Builder.Default
    private ConnectionType connectionType = ConnectionType.HTTP;

    public ConnectionType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(ConnectionType connectionType) {
        this.connectionType = connectionType;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
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
        return getQueryTimeout > 0
               ? getQueryTimeout
               : DEFAULT_TIMEOUT;
    }

    public enum ConnectionType {
        HTTP,
        HTTPS
    }

}
