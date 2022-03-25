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

import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

/***
 Created by nitish.goyal on 13/08/18
 ***/
@Slf4j
@Singleton
@Order(35)
public class CardinalityCalculationJobManager extends BaseJobManager {

    public static final String CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID = "cardinality_calculation_failure_report";
    public static final String CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID = "cardinality_calculation_time_exceeded";
    private final CardinalityCalculationFactory cardinalityCalculationFactory;
    private final CardinalityConfig cardinalityConfig;


    @Inject
    public CardinalityCalculationJobManager(CardinalityConfig cardinalityConfig,
                                            HazelcastConnection hazelcastConnection,
                                            ScheduledExecutorService scheduledExecutorService,
                                            CardinalityCalculationFactory cardinalityCalculationFactory) {
        super(cardinalityConfig, scheduledExecutorService, hazelcastConnection);
        this.cardinalityCalculationFactory = cardinalityCalculationFactory;
        this.cardinalityConfig = cardinalityConfig;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(cardinalityCalculationFactory.getRunnable(),
                new LockConfiguration(cardinalityConfig.getJobName(), lockAtMostUntil));
    }


}
