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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
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
    private Config hazelcastConfig;

    public HazelcastConnection(ClusterConfig clusterConfig, ObjectMapper mapper) throws Exception {
        this.clusterConfig = clusterConfig;
        this.mapper = mapper;
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        Config hzConfig = new Config();
        hzConfig.getGroupConfig().setName(clusterConfig.getName());
        hzConfig.setManagementCenterConfig(new ManagementCenterConfig());
        hzConfig.setInstanceName(String.format("foxtrot-%s-%d", hostName, System.currentTimeMillis()));
        if (clusterConfig.isDisableMulticast()) {
            hzConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            for (String member : clusterConfig.getMembers()) {
                hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(member);
            }
            hzConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        }
        this.hazelcastConfig = hzConfig;
    }

    @Override
    public void start() throws Exception {
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastConfig);
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

    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }
}
