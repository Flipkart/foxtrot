package com.flipkart.foxtrot.server.cluster;

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IMap;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class.getSimpleName());

    private static final String MAP_NAME = "__FOXTROT_MEMBERS_MAP";
    private static final int MAP_REFRESH_TIME = 5;
    private final ClusterMember clusterMember;

    private IMap<String, ClusterMember> members;
    private HazelcastConnection hazelcastConnection;
    private final List<HealthCheck> healthChecks;
    private ScheduledExecutorService executor;

    public ClusterManager(HazelcastConnection connection,
                          List<HealthCheck> healthChecks,
                          HttpConfiguration httpConfiguration) throws Exception {
        this.hazelcastConnection = connection;
        this.healthChecks = healthChecks;
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setTimeToLiveSeconds(MAP_REFRESH_TIME + 2); //Reduce jitter
        mapConfig.setBackupCount(1);
        mapConfig.setAsyncBackupCount(2);
        mapConfig.setEvictionPolicy(EvictionPolicy.NONE);
        hazelcastConnection.getHazelcastConfig().getMapConfigs().put(MAP_NAME, mapConfig);

        String hostname = Inet4Address.getLocalHost().getCanonicalHostName();
        executor = Executors.newScheduledThreadPool(1);
        clusterMember = new ClusterMember(hostname, httpConfiguration.getPort());
    }

    @Override
    public void start() throws Exception {
        members = hazelcastConnection.getHazelcast().getMap(MAP_NAME);
        executor.scheduleWithFixedDelay(new NodeDataUpdater(
                healthChecks, members, clusterMember), 0, MAP_REFRESH_TIME, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        members.remove(clusterMember.toString());
    }

    public Collection<ClusterMember> getMembers() {
        return members.values();
    }

    private static final class NodeDataUpdater implements Runnable {
        private final List<HealthCheck> healthChecks;
        private IMap<String, ClusterMember> members;
        private final ClusterMember clusterMember;

        private NodeDataUpdater(List<HealthCheck> healthChecks, IMap<String, ClusterMember> members, ClusterMember clusterMember) {
            this.healthChecks = ImmutableList.copyOf(healthChecks);
            this.members = members;
            this.clusterMember = clusterMember;
        }

        @Override
        public void run() {
            if (null == members) {
                logger.error("Map not yet initialized.");
                return;
            }
            try {
                boolean isHealthy = true;
                for (HealthCheck healthCheck : healthChecks) {
                    isHealthy &= healthCheck.execute().isHealthy();
                }
                if (isHealthy) {
                    members.put(clusterMember.toString(), clusterMember);
                    logger.debug("Service is healthy. Registering to map.");
                }
            } catch (Exception e) {
                logger.error("Error updating value in map: ", e);
            }
        }
    }
}
