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

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.marathon.hazelcast.servicediscovery.MarathonDiscoveryStrategyFactory;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:01 PM
 */
@Order(10)
@Singleton
public class HazelcastConnection implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(HazelcastConnection.class.getSimpleName());

    public static final String HEALTHCHECK_MAP = "healthCheck";

    private HazelcastInstance hazelcast;
    private Config hazelcastConfig;

    @Inject
    public HazelcastConnection(ClusterConfig clusterConfig) throws UnknownHostException {
        Config hzConfig = new Config();
        hzConfig.getGroupConfig()
                .setName(clusterConfig.getName());
        switch (clusterConfig.getDiscovery()
                .getType()) {
            case FOXTROT_SIMPLE:
                final String hostName = InetAddress.getLocalHost()
                        .getCanonicalHostName();
                hzConfig.setInstanceName(String.format("foxtrot-%s-%d", hostName, System.currentTimeMillis()));
                SimpleClusterDiscoveryConfig simpleClusterDiscoveryConfig = (SimpleClusterDiscoveryConfig)clusterConfig.getDiscovery();
                if(simpleClusterDiscoveryConfig.isDisableMulticast()) {
                    hzConfig.getNetworkConfig()
                            .getJoin()
                            .getMulticastConfig()
                            .setEnabled(false);
                    for(String member : simpleClusterDiscoveryConfig.getMembers()) {
                        hzConfig.getNetworkConfig()
                                .getJoin()
                                .getTcpIpConfig()
                                .addMember(member);
                    }
                    hzConfig.getNetworkConfig()
                            .getJoin()
                            .getTcpIpConfig()
                            .setEnabled(true);
                }
                break;
            case FOXTROT_MARATHON:
                MarathonClusterDiscoveryConfig marathonClusterDiscoveryConfig =
                        (MarathonClusterDiscoveryConfig)clusterConfig.getDiscovery();
                String appId = marathonClusterDiscoveryConfig.getApp()
                        .replace("/", "")
                        .trim();
                hzConfig.getGroupConfig()
                        .setName("foxtrot");
                hzConfig.getGroupConfig()
                        .setPassword("foxtrot");
                hzConfig.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
                hzConfig.setProperty(GroupProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");
                hzConfig.setProperty(GroupProperty.SOCKET_CLIENT_BIND_ANY.getName(), "true");
                hzConfig.setProperty(GroupProperty.SOCKET_BIND_ANY.getName(), "true");

                NetworkConfig networkConfig = hzConfig.getNetworkConfig();
                networkConfig.setPublicAddress(System.getenv("HOST") + ":" + System.getenv("PORT_5701"));
                JoinConfig joinConfig = networkConfig.getJoin();
                joinConfig.getTcpIpConfig()
                        .setEnabled(false);
                joinConfig.getMulticastConfig()
                        .setEnabled(false);
                joinConfig.getAwsConfig()
                        .setEnabled(false);

                DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig();
                DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new MarathonDiscoveryStrategyFactory());
                discoveryStrategyConfig.addProperty("marathon-endpoint", marathonClusterDiscoveryConfig.getEndpoint());
                discoveryStrategyConfig.addProperty("app-id", appId);
                discoveryStrategyConfig.addProperty("port-index", marathonClusterDiscoveryConfig.getPortIndex());
                discoveryConfig.addDiscoveryStrategyConfig(discoveryStrategyConfig);
                break;
            case FOXTROT_AWS:
                AwsClusterDiscoveryConfig awsClusterDiscoveryConfig = (AwsClusterDiscoveryConfig)clusterConfig.getDiscovery();
                NetworkConfig hazelcastConfigNetworkConfig = hzConfig.getNetworkConfig();
                JoinConfig hazelcastConfigNetworkConfigJoin = hazelcastConfigNetworkConfig.getJoin();
                hazelcastConfigNetworkConfigJoin.getTcpIpConfig()
                        .setEnabled(false);
                hazelcastConfigNetworkConfigJoin.getMulticastConfig()
                        .setEnabled(false);
                AwsConfig awsConfig = new AwsConfig();
                awsConfig.setAccessKey(awsClusterDiscoveryConfig.getAccessKey());
                awsConfig.setConnectionTimeoutSeconds(awsClusterDiscoveryConfig.getConnectionTimeoutSeconds());
                awsConfig.setHostHeader(awsClusterDiscoveryConfig.getHostHeader());
                awsConfig.setIamRole(awsClusterDiscoveryConfig.getIamRole());
                awsConfig.setRegion(awsClusterDiscoveryConfig.getRegion());
                awsConfig.setSecurityGroupName(awsClusterDiscoveryConfig.getSecurityGroupName());
                awsConfig.setSecretKey(awsClusterDiscoveryConfig.getSecretKey());
                awsConfig.setTagKey(awsClusterDiscoveryConfig.getTagKey());
                awsConfig.setTagValue(awsClusterDiscoveryConfig.getTagValue());
                hazelcastConfigNetworkConfigJoin.setAwsConfig(awsConfig);
                hazelcastConfigNetworkConfigJoin.getAwsConfig()
                        .setEnabled(true);
                break;
            case FOXTROT_KUBERNETES:
                logger.info("Using Kubernetes");
                JoinConfig kbConfig = hzConfig.getNetworkConfig().getJoin();
                kbConfig.getMulticastConfig().setEnabled(false);
                kbConfig.getKubernetesConfig().setEnabled(true);
                break;
            default:
                logger.warn("Invalid discovery config");
        }
        this.hazelcastConfig = hzConfig;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Hazelcast Instance");
        configureHealthcheck();
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastConfig);
        logger.info("Started Hazelcast Instance");
    }

    private void configureHealthcheck() {
        MapConfig mapConfig = new MapConfig(HEALTHCHECK_MAP);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        hazelcastConfig.addMapConfig(mapConfig);
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
