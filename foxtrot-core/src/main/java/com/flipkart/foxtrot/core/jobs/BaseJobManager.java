package com.flipkart.foxtrot.core.jobs;

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import io.dropwizard.lifecycle.Managed;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.hazelcast.HazelcastLockProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Calendar;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/***
 Created by nitish.goyal on 11/09/18
 ***/
public abstract class BaseJobManager implements Managed {

    private static final int LOCK_AT_MOST = 120;
    private static final String TIME_ZONE = "Asia/Kolkata";
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseJobManager.class.getSimpleName());
    private final BaseJobConfig baseJobConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private final HazelcastConnection hazelcastConnection;

    public BaseJobManager(
            BaseJobConfig baseJobConfig, ScheduledExecutorService scheduledExecutorService,
            HazelcastConnection hazelcastConnection) {
        this.baseJobConfig = baseJobConfig;
        this.scheduledExecutorService = scheduledExecutorService;
        this.hazelcastConnection = hazelcastConnection;
    }

    @Override
    public void start() {
        LOGGER.info("Starting {} Manager", baseJobConfig.getJobName());
        if (!baseJobConfig.isActive()) {
            LOGGER.info("Config is not active. Hence, aborting the {} job", baseJobConfig.getJobName());
        }
        LOGGER.info("Scheduling {} Job", baseJobConfig.getJobName());
        LocalDateTime localNow = LocalDateTime.now();
        Calendar now = Calendar.getInstance();
        String timeZone = now.getTimeZone()
                .getID();
        if (StringUtils.isEmpty(timeZone)) {
            timeZone = TIME_ZONE;
        }
        ZoneId currentZone = ZoneId.of(timeZone);
        ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
        ZonedDateTime timeToRunJob = zonedNow.withHour(baseJobConfig.getInitialDelay())
                .withMinute(0)
                .withSecond(0);
        if (zonedNow.compareTo(timeToRunJob) > 0) {
            timeToRunJob = timeToRunJob.plusDays(1);
        }

        Duration duration = Duration.between(zonedNow, timeToRunJob);
        long initialDelay = duration.getSeconds();

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                LockingTaskExecutor executor = new DefaultLockingTaskExecutor(
                        new HazelcastLockProvider(hazelcastConnection.getHazelcast()));
                int lockAtMost = LOCK_AT_MOST;
                if (baseJobConfig.getLockAtMostInMinutes() != 0) {
                    lockAtMost = baseJobConfig.getLockAtMostInMinutes();
                }
                Instant lockAtMostUntil = Instant.now()
                        .plusSeconds(TimeUnit.MINUTES.toSeconds(lockAtMost));
                runImpl(executor, lockAtMostUntil);
            }
            catch (Exception e) {
                LOGGER.error("Error occurred while running the job : ", e);
            }
        }, initialDelay, baseJobConfig.getInterval(), TimeUnit.SECONDS);

        LOGGER.info("Scheduled {} Job", baseJobConfig.getJobName());
    }

    @Override
    public void stop() {
        LOGGER.info("Stopped {} Job Manager", baseJobConfig.getJobName());
    }

    protected abstract void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil);

}
