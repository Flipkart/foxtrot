package com.flipkart.foxtrot.core.querystore.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.yammer.dropwizard.lifecycle.Managed;

import java.net.InetAddress;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:01 PM
 */
public class HazelcastConnection implements Managed {

    private ClusterConfig clusterConfig;
    private HazelcastInstance hazelcast;

    public HazelcastConnection(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    @Override
    public void start() throws Exception {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        Config hzConfig = new Config();
        hzConfig.getGroupConfig().setName(clusterConfig.getName());
        hzConfig.setInstanceName(String.format("foxtrot-%s-%d", hostName, System.currentTimeMillis()));
        if(clusterConfig.isDisableMulticast()) {
            hzConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            for(String member: clusterConfig.getMembers()) {
                hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(member);
            }
            hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        }
        hazelcast = Hazelcast.newHazelcastInstance(hzConfig);
    }

    @Override
    public void stop() throws Exception {
        hazelcast.shutdown();
    }
}
