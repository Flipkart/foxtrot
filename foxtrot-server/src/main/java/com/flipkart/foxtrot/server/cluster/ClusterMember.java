package com.flipkart.foxtrot.server.cluster;

import java.io.Serializable;

public class ClusterMember implements Serializable {
    private String host;
    private int port;

    public ClusterMember() {
    }

    public ClusterMember(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }
}
