package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.ActionConstants;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class DataDeletionJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionJob.class.getSimpleName());

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        logger.info("Starting Deletion Job");
        QueryStore queryStore = (QueryStore) jobExecutionContext.getJobDetail().getJobDataMap()
                .get(ActionConstants.JOB_QUERY_STORE_KEY);
        try {
            queryStore.cleanupAll();
        } catch (QueryStoreException ex) {
            logger.error("Deletion Job Failed ", ex);
            throw new JobExecutionException(ex);
        }
        logger.info("Finished Deletion Job");
    }
}
