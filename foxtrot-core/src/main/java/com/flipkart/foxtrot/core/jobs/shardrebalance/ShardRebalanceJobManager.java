package com.flipkart.foxtrot.core.jobs.shardrebalance;


import com.flipkart.foxtrot.core.config.ShardRebalanceJobConfig;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.rebalance.ClusterRebalanceService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
@Singleton
@Order(26)
public class ShardRebalanceJobManager extends BaseJobManager {

    private final ShardRebalanceJobConfig config;
    private final ClusterRebalanceService clusterRebalanceService;

    @Inject
    public ShardRebalanceJobManager(ShardRebalanceJobConfig shardRebalanceJobConfig,
                                    ClusterRebalanceService clusterRebalanceService,
                                    ScheduledExecutorService scheduledExecutorService,
                                    HazelcastConnection hazelcastConnection) {
        super(shardRebalanceJobConfig, scheduledExecutorService, hazelcastConnection);
        this.config = shardRebalanceJobConfig;
        this.clusterRebalanceService = clusterRebalanceService;
    }


    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(clusterRebalanceService::rebalanceShards,
                new LockConfiguration(config.getJobName(), lockAtMostUntil));

    }
}
