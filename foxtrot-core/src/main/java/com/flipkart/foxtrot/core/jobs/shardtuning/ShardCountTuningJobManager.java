package com.flipkart.foxtrot.core.jobs.shardtuning;

import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.shardtuning.ShardCountTuningService;
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
@Order(28)
public class ShardCountTuningJobManager extends BaseJobManager {

    private final ShardCountTuningJobConfig config;
    private final ShardCountTuningService shardCountTuningService;

    @Inject
    public ShardCountTuningJobManager(ShardCountTuningJobConfig shardCountTuningJobConfig,
                                      ShardCountTuningService shardCountTuningService,
                                      ScheduledExecutorService scheduledExecutorService,
                                      HazelcastConnection hazelcastConnection) {
        super(shardCountTuningJobConfig, scheduledExecutorService, hazelcastConnection);
        this.config = shardCountTuningJobConfig;
        this.shardCountTuningService = shardCountTuningService;
    }


    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(shardCountTuningService::tuneShardCount,
                new LockConfiguration(config.getJobName(), lockAtMostUntil));

    }
}
