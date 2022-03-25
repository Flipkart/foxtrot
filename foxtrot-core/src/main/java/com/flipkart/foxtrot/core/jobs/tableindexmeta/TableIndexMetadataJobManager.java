package com.flipkart.foxtrot.core.jobs.tableindexmeta;

import com.flipkart.foxtrot.core.config.TableIndexMetadataJobConfig;
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
@Order(27)
public class TableIndexMetadataJobManager extends BaseJobManager {

    private final TableIndexMetadataJobConfig tableIndexMetadataJobConfig;
    private final TableIndexMetadataService tableIndexMetadataService;

    @Inject
    public TableIndexMetadataJobManager(TableIndexMetadataJobConfig tableIndexMetadataJobConfig,
                                        TableIndexMetadataService tableIndexMetadataService,
                                        ScheduledExecutorService scheduledExecutorService,
                                        HazelcastConnection hazelcastConnection) {
        super(tableIndexMetadataJobConfig, scheduledExecutorService, hazelcastConnection);
        this.tableIndexMetadataJobConfig = tableIndexMetadataJobConfig;
        this.tableIndexMetadataService = tableIndexMetadataService;
    }


    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(() -> tableIndexMetadataService.syncTableIndexMetadata(
                tableIndexMetadataJobConfig.getOldMetadataSyncDurationInDays()),
                new LockConfiguration(tableIndexMetadataJobConfig.getJobName(), lockAtMostUntil));

    }
}
