package com.flipkart.foxtrot.server.managed;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.server.jobs.DataDeletionJob;
import com.flipkart.foxtrot.server.util.Constants;
import com.yammer.dropwizard.lifecycle.Managed;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class TableDataManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(TableDataManager.class.getSimpleName());
    final Scheduler scheduler;
    final JobDetail jobDetail;
    final Trigger trigger;

    public TableDataManager(TableMetadataManager metadataManager, QueryStore queryStore) throws SchedulerException {
        this.jobDetail = JobBuilder.newJob(DataDeletionJob.class).withIdentity("DataDeletionJob").build();
        this.jobDetail.getJobDataMap().put(Constants.JOB_QUERY_STORE_KEY, queryStore);
        this.jobDetail.getJobDataMap().put(Constants.JOB_TABLE_METADATA_MANAGER_KEY, metadataManager);
        this.trigger = TriggerBuilder.newTrigger()
                .withIdentity("DataDeletionTrigger")
                .withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(0, 0))
                .build();
        this.scheduler = new StdSchedulerFactory().getScheduler();
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Quartz Scheduler");
        scheduler.start();
        logger.info("Started Quartz Scheduler");
        logger.info("Scheduling Data Deletion Job");
        scheduler.scheduleJob(jobDetail, trigger);
        logger.info("Scheduled Data Deletion Job");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping Quartz Scheduler");
        scheduler.shutdown();
        logger.info("Stopped Quartz Scheduler");
    }
}
