package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import io.dropwizard.lifecycle.Managed;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.hazelcast.HazelcastLockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by rishabh.goyal on 07/07/14.
 */

public class DataDeletionManager implements Managed {

    private static final int MAX_TIME_TO_RUN_TASK_IN_HOURS = 2;
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionManager.class.getSimpleName());

    private final DataDeletionManagerConfig config;
    private final QueryStore queryStore;
    private final ScheduledExecutorService scheduledExecutorService;
    private final HazelcastConnection hazelcastConnection;

    public DataDeletionManager(DataDeletionManagerConfig deletionManagerConfig, QueryStore queryStore,
                               ScheduledExecutorService scheduledExecutorService, HazelcastConnection hazelcastConnection) {
        this.config = deletionManagerConfig;
        this.queryStore = queryStore;
        this.hazelcastConnection = hazelcastConnection;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Deletion Manager");
        if(config.isActive()) {
            logger.info("Scheduling data deletion Job");
            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LockingTaskExecutor executor =
                            new DefaultLockingTaskExecutor(new HazelcastLockProvider(hazelcastConnection.getHazelcast()));
                    Instant lockAtMostUntil = Instant.now().plusSeconds(TimeUnit.HOURS.toSeconds(MAX_TIME_TO_RUN_TASK_IN_HOURS));
                    executor.executeWithLock(new DataDeletionTask(queryStore), new LockConfiguration("dataDeletion", lockAtMostUntil));

                }
            }, config.getInitialDelay(), config.getInterval(), TimeUnit.SECONDS);
            logger.info("Scheduled data deletion Job");
        } else {
            logger.info("Not scheduling data deletion Job");
        }
        logger.info("Started Deletion Manager");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopped Deletion Manager");
    }
}
