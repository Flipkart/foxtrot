package com.flipkart.foxtrot.core.reroute;

import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

/***
 Created by mudit.g on Sep, 2019
 ***/
public class ClusterRerouteJob extends BaseJobManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRerouteJob.class.getSimpleName());

    private final ClusterRerouteConfig clusterRerouteConfig;
    private final ClusterRerouteManager clusterRerouteManager;


    public ClusterRerouteJob(
            ScheduledExecutorService scheduledExecutorService, ClusterRerouteConfig clusterRerouteConfig,
            ClusterRerouteManager clusterRerouteManager, HazelcastConnection hazelcastConnection) {
        super(clusterRerouteConfig, scheduledExecutorService, hazelcastConnection);
        this.clusterRerouteConfig = clusterRerouteConfig;
        this.clusterRerouteManager = clusterRerouteManager;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                clusterRerouteManager.reallocate();
            }
            catch (Exception e) {
                logger.info("Failed to reallocate shards. {}", e);
            }
        }, new LockConfiguration(clusterRerouteConfig.getJobName(), lockAtMostUntil));
    }

}
