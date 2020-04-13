package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import java.util.TimerTask;
import net.javacrumbs.shedlock.core.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class DataDeletionTask extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(DataDeletionTask.class.getSimpleName());
    private final QueryStore queryStore;

    public DataDeletionTask(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    @SchedulerLock(name = "dataDeletion")
    @Override
    public void run() {
        logger.info("Starting Deletion Job");
        try {
            queryStore.cleanupAll();
        }
        catch (FoxtrotException ex) {
            logger.error("Deletion Job Failed ", ex);
        }
        logger.info("Finished Deletion Job");
    }
}
