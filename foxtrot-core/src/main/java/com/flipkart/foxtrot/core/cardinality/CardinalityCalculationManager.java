package com.flipkart.foxtrot.core.cardinality;
/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import io.dropwizard.lifecycle.Managed;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.hazelcast.HazelcastLockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.concurrent.TimeUnit;

/***
 Created by nitish.goyal on 13/08/18
 ***/
public class CardinalityCalculationManager implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(CardinalityCalculationManager.class.getSimpleName());
    private static final String TIME_ZONE = "Asia/Kolkata";
    private static final int MAX_TIME_TO_RUN_TASK_IN_HOURS = 1;

    private final TableMetadataManager tableMetadataManager;
    private final CardinalityConfig cardinalityConfig;
    private final HazelcastConnection hazelcastConnection;

    public CardinalityCalculationManager(TableMetadataManager tableMetadataManager, CardinalityConfig cardinalityConfig, HazelcastConnection hazelcastConnection) {
        this.tableMetadataManager = tableMetadataManager;
        this.cardinalityConfig = cardinalityConfig;
        this.hazelcastConnection = hazelcastConnection;
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting Deletion Manager");
        if (cardinalityConfig.isActive()) {
            LOGGER.info("Scheduling cardinality calculation job");
            LocalDateTime localNow = LocalDateTime.now();
            ZoneId currentZone = ZoneId.of(TIME_ZONE);
            ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
            ZonedDateTime zonedNext5 = zonedNow.withHour(cardinalityConfig.getInitialDelay()).withMinute(0).withSecond(0);
            if (zonedNow.compareTo(zonedNext5) > 0)
                zonedNext5 = zonedNext5.plusDays(1);

            Duration duration = Duration.between(zonedNow, zonedNext5);
            long initialDelay = duration.getSeconds();

            LockingTaskExecutor executor = new DefaultLockingTaskExecutor(new HazelcastLockProvider(hazelcastConnection.getHazelcast()));
            Instant lockAtMostUntil = Instant.now().plusSeconds(initialDelay).plusSeconds(TimeUnit.HOURS.toSeconds(MAX_TIME_TO_RUN_TASK_IN_HOURS));
            executor.executeWithLock(new CardinalityCalculationRunnable(tableMetadataManager), new LockConfiguration("cardinalityCalculation", lockAtMostUntil));
            LOGGER.info("Scheduled  cardinality calculation job");
        } else {
            LOGGER.info("Not scheduling cardinality calculation job");
        }
        LOGGER.info("Started Cardinality Manager");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping Cardinality Manager");
        LOGGER.info("Stopped Cardinality Manager");
    }
}
