package com.flipkart.foxtrot.core.jobs.tableindexmeta;

import com.flipkart.foxtrot.core.config.IndexMetadataCleanupJobConfig;
import com.flipkart.foxtrot.core.indexmeta.TableIndexMetadataService;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
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
@Order(30)
public class TableIndexMetadataCleanupJobManager extends BaseJobManager {

    private final IndexMetadataCleanupJobConfig indexMetadataCleanupJobConfig;
    private final TableIndexMetadataService tableIndexMetadataService;

    @Inject
    public TableIndexMetadataCleanupJobManager(IndexMetadataCleanupJobConfig indexMetadataCleanupJobConfig,
                                               TableIndexMetadataService tableIndexMetadataService,
                                               ScheduledExecutorService scheduledExecutorService,
                                               HazelcastConnection hazelcastConnection) {
        super(indexMetadataCleanupJobConfig, scheduledExecutorService, hazelcastConnection);
        this.indexMetadataCleanupJobConfig = indexMetadataCleanupJobConfig;
        this.tableIndexMetadataService = tableIndexMetadataService;
    }


    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(
                () -> tableIndexMetadataService.cleanupIndexMetadata(indexMetadataCleanupJobConfig.getRetentionDays()),
                new LockConfiguration(indexMetadataCleanupJobConfig.getJobName(), lockAtMostUntil));

    }
}
