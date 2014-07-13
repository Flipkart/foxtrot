package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.ActionConstants;
import com.yammer.dropwizard.lifecycle.Managed;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class DataDeletionManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionManager.class.getSimpleName());
    final Scheduler scheduler;
    final JobDetail jobDetail;
    final Trigger trigger;
    final DataDeletionManagerConfig config;

    public DataDeletionManager(DataDeletionManagerConfig deletionManagerConfig, QueryStore queryStore) throws SchedulerException {
        this.config = deletionManagerConfig;
        this.jobDetail = JobBuilder.newJob(DataDeletionJob.class).withIdentity("DataDeletionJob").build();
        this.jobDetail.getJobDataMap().put(ActionConstants.JOB_QUERY_STORE_KEY, queryStore);
        this.trigger = TriggerBuilder.newTrigger()
                .withIdentity("DataDeletionTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule(deletionManagerConfig.getDeletionSchedule()))
                .build();
        this.scheduler = new StdSchedulerFactory().getScheduler();
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Quartz Scheduler");
        scheduler.start();
        logger.info("Started Quartz Scheduler");
        if (config.isActive()) {
            logger.info("Scheduling data deletion Job");
            scheduler.scheduleJob(jobDetail, trigger);
            logger.info("Scheduled data deletion Job");
        } else {
            logger.info("Not scheduling data deletion Job");
        }
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping Quartz Scheduler");
        scheduler.shutdown();
        logger.info("Stopped Quartz Scheduler");
    }
}
