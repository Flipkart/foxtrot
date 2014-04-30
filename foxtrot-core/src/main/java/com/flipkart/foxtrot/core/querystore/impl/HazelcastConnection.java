package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;

import java.net.InetAddress;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:01 PM
 */

@CoverageIgnore
public class HazelcastConnection implements Managed {

    private final ClusterConfig clusterConfig;
    private HazelcastInstance hazelcast;
    private final ObjectMapper mapper;

    public HazelcastConnection(ClusterConfig clusterConfig, ObjectMapper mapper) {
        this.clusterConfig = clusterConfig;
        this.mapper = mapper;
    }

    @Override
    public void start() throws Exception {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        Config hzConfig = new Config();
        hzConfig.getGroupConfig().setName(clusterConfig.getName());
        hzConfig.setInstanceName(String.format("foxtrot-%s-%d", hostName, System.currentTimeMillis()));
        if (clusterConfig.isDisableMulticast()) {
            hzConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            for (String member : clusterConfig.getMembers()) {
                hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(member);
            }
            hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        }
        hazelcast = Hazelcast.newHazelcastInstance(hzConfig);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(this, mapper));
    }

    @Override
    public void stop() throws Exception {
        hazelcast.shutdown();
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }
}
