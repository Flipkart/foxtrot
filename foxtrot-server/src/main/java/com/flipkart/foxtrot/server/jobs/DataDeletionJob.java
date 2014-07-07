package com.flipkart.foxtrot.server.jobs;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.server.util.Constants;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class DataDeletionJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionJob.class.getSimpleName());

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        logger.info("Starting Deletion Job");
        QueryStore queryStore = (QueryStore) jobExecutionContext.getJobDetail().getJobDataMap().get(Constants.JOB_QUERY_STORE_KEY);
        TableMetadataManager metadataManager = (TableMetadataManager) jobExecutionContext.getJobDetail().getJobDataMap().get(Constants.JOB_TABLE_METADATA_MANAGER_KEY);
        List<Table> tables;
        Map<String, Integer> map = new HashMap<String, Integer>();
        try {
            tables = metadataManager.get();
            for (Table table : tables) {
                map.put(table.getName(), (int) table.getTtl());
            }
        } catch (Exception ex) {
            logger.error("Unable to Fetch Table List", ex);
            throw new JobExecutionException("Unable to Fetch Table List", ex);
        }
        queryStore.delete(map);
        logger.info("Finished Deletion Job");
    }
}
