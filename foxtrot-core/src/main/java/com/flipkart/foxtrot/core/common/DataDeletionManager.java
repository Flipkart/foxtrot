package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by rishabh.goyal on 07/07/14.
 */

@Slf4j
@Singleton
@Order(25)
public class DataDeletionManager extends BaseJobManager {

    private final DataDeletionManagerConfig config;
    private final QueryStore queryStore;

    @Inject
    public DataDeletionManager(DataDeletionManagerConfig deletionManagerConfig,
                               QueryStore queryStore,
                               ScheduledExecutorService scheduledExecutorService,
                               HazelcastConnection hazelcastConnection) {
        super(deletionManagerConfig, scheduledExecutorService, hazelcastConnection);
        this.config = deletionManagerConfig;
        this.queryStore = queryStore;
    }


    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(new DataDeletionTask(queryStore),
                new LockConfiguration(config.getJobName(), lockAtMostUntil));

    }
}
