package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 * Created by rishabh.goyal on 07/07/14.
 */

public class DataDeletionManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionManager.class.getSimpleName());
    final DataDeletionManagerConfig config;
    final Timer timer;
    final QueryStore queryStore;

    public DataDeletionManager(DataDeletionManagerConfig deletionManagerConfig, QueryStore queryStore) {
        this.config = deletionManagerConfig;
        this.queryStore = queryStore;
        this.timer = new Timer(true);
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Deletion Manager");
        if (config.isActive()) {
            logger.info("Scheduling data deletion Job");
            this.timer.scheduleAtFixedRate(new DataDeletionTask(queryStore),
                    config.getInitialDelay() * 1000L,
                    config.getInterval() * 1000L);
            logger.info("Scheduled data deletion Job");
        } else {
            logger.info("Not scheduling data deletion Job");
        }
        logger.info("Started Deletion Manager");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping Deletion Manager");
        timer.cancel();
        logger.info("Stopped Deletion Manager");
    }
}
