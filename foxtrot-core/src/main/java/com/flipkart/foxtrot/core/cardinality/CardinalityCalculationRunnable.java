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

import com.flipkart.foxtrot.common.exception.BadRequestException;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.CardinalityStatus;
import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.SchedulerLock;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.function.Supplier;

import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationJobManager.CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID;

/***
 Created by nitish.goyal on 13/08/18
 ***/
@Slf4j
@SuppressWarnings({"squid:S107"})
public class CardinalityCalculationRunnable implements Runnable {

    private final TableMetadataManager tableMetadataManager;
    private final CardinalityCalculationService cardinalityCalculationService;
    private final int maxTimeToRunJobInMinutes;
    private final int retryCount;
    private final EmailClient emailClient;
    private final RichEmailBuilder richEmailBuilder;
    private final EmailConfig emailConfig;
    private final Supplier<Set<String>> tablesSupplier;

    public CardinalityCalculationRunnable(final int maxTimeToRunJobInMinutes,
                                          final int retryCount,
                                          final Supplier<Set<String>> tablesSupplier,
                                          final TableMetadataManager tableMetadataManager,
                                          final CardinalityCalculationService cardinalityCalculationService,
                                          final EmailClient emailClient,
                                          final RichEmailBuilder richEmailBuilder,
                                          final EmailConfig emailConfig) {
        this.maxTimeToRunJobInMinutes = maxTimeToRunJobInMinutes;
        this.retryCount = retryCount;
        this.tableMetadataManager = tableMetadataManager;
        this.cardinalityCalculationService = cardinalityCalculationService;
        this.emailClient = emailClient;
        this.richEmailBuilder = richEmailBuilder;
        this.emailConfig = emailConfig;
        this.tablesSupplier = tablesSupplier;
    }

    @SchedulerLock(name = "cardinalityCalculation")
    @Override
    public void run() {
        try {
            Set<String> tablesToRun = tablesSupplier.get();
            runCardinalityTask(tablesToRun);
        } catch (Exception exception) {
            log.error("Error occurred while running cardinality estimation job", exception);
            sendJobFailureEmail(exception);
        }
    }

    private void runCardinalityTask(Set<String> tables) {
        Instant jobStart = Instant.now();

        boolean timeLimitExceeded = false;

        for (String table : tables) {
            int retries = retryCount;
            boolean jobDone = false;
            Instant calculationStart = Instant.now();
            while (!timeLimitExceeded && !jobDone && retries > 0) {
                try {
                    log.info("Calculating Cardinality estimation data for table: {}", table);
                    CardinalityCalculationResult cardinalityCalculationResult = tableMetadataManager.calculateCardinality(
                            table);
                    log.info("Cardinality estimation data calculated for table: {}", table);
                    if (cardinalityCalculationResult.isEvenPartiallySuccessful()) {
                        jobDone = true;

                        updateAuditInfo(table, calculationStart, cardinalityCalculationResult.getStatus());

                        // even if cardinality calculation is partially successful, send mail with exceptions occurred
                        logAndSendFailureEmail(table, cardinalityCalculationResult);
                    } else {
                        // if cardinality calculation is not even partially successful then
                        // we keep on retrying and send mail only when we exhausted retry count
                        retries--;
                        if (retries == 0) {
                            updateAuditInfo(table, calculationStart, CardinalityStatus.FAILED);
                            logAndSendFailureEmail(table, cardinalityCalculationResult);
                        }
                    }

                } catch (BadRequestException badRequestException) {
                    log.error("Ignore bad request exception in cardinality calculatiEon for table : {}", table);
                    jobDone = true;
                } catch (Exception exception) {
                    retries--;
                    if (retries == 0) {
                        log.error(
                                "Error occurred while calculating cardinality estimation data for table:{}, retryCount :{}, error: ",
                                table, retryCount, exception);
                        updateAuditInfo(table, calculationStart, CardinalityStatus.FAILED);
                        sendJobFailureEmail(exception);
                    }
                } finally {
                    Instant now = Instant.now();
                    Duration timeElapsed = Duration.between(jobStart, now);
                    if (timeElapsed.compareTo(Duration.ofMinutes(maxTimeToRunJobInMinutes)) > 0) {
                        log.info("Elapsed {} seconds while running cardinality calculation job, "
                                        + "more than configured max time :{} seconds, hence breaking the loop",
                                timeElapsed.getSeconds(), maxTimeToRunJobInMinutes * 60L);
                        sendJobTimeLimitExceededEmail(timeElapsed.getSeconds(), maxTimeToRunJobInMinutes * 60L);
                        timeLimitExceeded = true;
                    }
                }
            }
        }
    }

    private void updateAuditInfo(String table,
                                 Instant start,
                                 CardinalityStatus status) {
        Instant now = Instant.now();
        Duration timeElapsed = Duration.between(start, now);
        CardinalityCalculationAuditInfo auditInfo = CardinalityCalculationAuditInfo.builder()
                .timeTakenInMillis(timeElapsed.toMillis())
                .status(status)
                .updatedAt(new Date())
                .build();

        cardinalityCalculationService.updateAuditInfo(table, auditInfo);
    }

    private void logAndSendFailureEmail(String table,
                                        CardinalityCalculationResult cardinalityCalculationResult) {
        if (!CollectionUtils.isNullOrEmpty(cardinalityCalculationResult.getErrors())) {
            for (Exception exception : cardinalityCalculationResult.getErrors()) {
                log.error(
                        "Error occurred while calculating cardinality estimation data for table:{}, retryCount :{}, error: ",
                        table, retryCount, exception);
                sendJobFailureEmail(exception);
            }
        }
    }

    private void sendJobTimeLimitExceededEmail(long elapsedTimeInSeconds,
                                               long maxTimeToRunJobInSeconds) {
        try {
            final Email email = richEmailBuilder.
                    build(CARDINALITY_CALCULATION_TIME_EXCEEDED_EMAIL_TEMPLATE_ID, Collections.emptyList(),
                            ImmutableMap.<String, Object>builder().put("elapsedTime", elapsedTimeInSeconds)
                                    .put("maxTimeToRunJob", maxTimeToRunJobInSeconds)
                                    .build());

            sendJobStatusEmail(email);
        } catch (Exception e) {
            log.error("Error while sending cardinality job time limit exceeded email, error : ", e);
        }


    }

    private void sendJobFailureEmail(Exception exception) {
        try {
            final Email email = richEmailBuilder.
                    build(CARDINALITY_CALCULATION_FAILURE_EMAIL_TEMPLATE_ID, Collections.emptyList(),
                            ImmutableMap.<String, Object>builder().put("message", exception.getMessage())
                                    .put("cause", exception.getCause() != null
                                            ? exception.getCause()
                                            : "null")
                                    .put("causeMessage", exception.getCause() != null
                                            ? exception.getCause()
                                            .getMessage()
                                            : "null")
                                    .build());
            sendJobStatusEmail(email);
        } catch (Exception e) {
            log.error("Error while sending cardinality job failure email, error : ", e);
        }

    }


    private void sendJobStatusEmail(Email email) {
        if (emailConfig.isCardinalityCalculationFailureEmailEnabled()) {
            if (null == email) {
                return;
            }
            emailClient.sendEmail(email);
        }
    }

}
