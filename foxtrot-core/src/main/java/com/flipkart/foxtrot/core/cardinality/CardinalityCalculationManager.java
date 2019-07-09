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

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/***
 Created by nitish.goyal on 13/08/18
 ***/
public class CardinalityCalculationManager extends BaseJobManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CardinalityCalculationManager.class.getSimpleName());
    private static final int MAX_TIME_TO_RUN_JOB = 120;

    private final TableMetadataManager tableMetadataManager;
    private final CardinalityConfig cardinalityConfig;

    public CardinalityCalculationManager(TableMetadataManager tableMetadataManager, CardinalityConfig cardinalityConfig,
                                         HazelcastConnection hazelcastConnection, ScheduledExecutorService scheduledExecutorService) {
        super(cardinalityConfig, scheduledExecutorService, hazelcastConnection);
        this.tableMetadataManager = tableMetadataManager;
        this.cardinalityConfig = cardinalityConfig;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                int maxTimeToRunJob = MAX_TIME_TO_RUN_JOB;
                if(cardinalityConfig.getMaxTimeToRunJobInMinutes() != 0) {
                    maxTimeToRunJob = cardinalityConfig.getMaxTimeToRunJobInMinutes();
                }
                Instant start = Instant.now();
                Set<String> tables = tableMetadataManager.get()
                        .stream()
                        .map(Table::getName)
                        .collect(Collectors.toSet());
                for(String table : tables) {
                    if(!tableMetadataManager.cardinalityCacheContains(table)) {
                        tableMetadataManager.getFieldMappings(table, true, true);
                        LOGGER.info("Cardinality calculated for table: " + table);
                    }
                    Instant now = Instant.now();
                    Duration timeElapsed = Duration.between(start, now);
                    if(timeElapsed.compareTo(Duration.ofMinutes(maxTimeToRunJob)) > 0) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error occurred while calculating cardinality " + e);
            }
        }, new LockConfiguration(cardinalityConfig.getJobName(), lockAtMostUntil));
    }
}
