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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:01 PM
 */

@CoverageIgnore
public class HazelcastConnection implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(HazelcastConnection.class.getSimpleName());

    private final ClusterConfig clusterConfig;
    private HazelcastInstance hazelcast;
    private Config hazelcastConfig;

    public HazelcastConnection(ClusterConfig clusterConfig) throws Exception {
        this.clusterConfig = clusterConfig;
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
        this.hazelcastConfig = hzConfig;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Hazelcast Instance");
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastConfig);
        logger.info("Started Hazelcast Instance");
    }

    @Override
    public void stop() throws Exception {
        hazelcast.shutdown();
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }
}
