package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableMetadataManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.common.util.MapUtils.mapSize;
import static com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.CardinalityStatus.*;

@Singleton
public class CardinalityCalculationFactory {

    private static final int MAX_TIME_TO_RUN_JOB = 120;
    private static final int BULK_UPDATE_CHUNK_SIZE = 100;
    private final TableMetadataManager tableMetadataManager;
    private final CardinalityCalculationService cardinalityCalculationService;
    private final CardinalityConfig cardinalityConfig;
    private final EmailClient emailClient;
    private final RichEmailBuilder richEmailBuilder;
    private final EmailConfig emailConfig;

    @Inject
    public CardinalityCalculationFactory(final TableMetadataManager tableMetadataManager,
                                         final CardinalityCalculationService cardinalityCalculationService,
                                         final CardinalityConfig cardinalityConfig,
                                         final EmailClient emailClient,
                                         final RichEmailBuilder richEmailBuilder,
                                         final EmailConfig emailConfig) {
        this.tableMetadataManager = tableMetadataManager;
        this.cardinalityCalculationService = cardinalityCalculationService;
        this.cardinalityConfig = cardinalityConfig;

        this.emailClient = emailClient;
        this.richEmailBuilder = richEmailBuilder;
        this.emailConfig = emailConfig;
    }

    public CardinalityCalculationRunnable getRunnable(int maxTimeToRunJobInMinutes,
                                                      int retryCount,
                                                      String table) {
        return getRunnable(() -> Collections.singleton(ElasticsearchUtils.getValidName(table)),
                maxTimeToRunJobInMinutes, retryCount);
    }

    public CardinalityCalculationRunnable getRunnable() {
        return getRunnable(tablesToRun(), cardinalityConfig.getMaxTimeToRunJobInMinutes(),
                cardinalityConfig.getRetryCount());
    }

    private CardinalityCalculationRunnable getRunnable(Supplier<Set<String>> tablesSupplier,
                                                       int maxTimeToRunJobInMinutes,
                                                       int retryCount) {
        return new CardinalityCalculationRunnable(getMaxTimeToRunJob(maxTimeToRunJobInMinutes),
                getRetryCount(retryCount), tablesSupplier, tableMetadataManager, cardinalityCalculationService,
                emailClient, richEmailBuilder, emailConfig);
    }

    private Supplier<Set<String>> tablesToRun() {
        return () -> {
            Set<String> tables = tableMetadataManager.get()
                    .stream()
                    .map(Table::getName)
                    .map(ElasticsearchUtils::getValidName)
                    .collect(Collectors.toSet());

            Map<String, CardinalityCalculationAuditInfo> auditInfoMap = cardinalityCalculationService.fetchAuditInfo();

            // if there are no pending tables then we completed trying for all tables
            // so reset audit info - mark PARTIALLY_COMPLETED and COMPLETED as PENDING now to start a new cycle
            if (!auditInfoMap.isEmpty() && auditInfoMap.entrySet()
                    .stream()
                    .noneMatch(entry -> PENDING.equals(entry.getValue()
                            .getStatus()))) {
                Set<String> tablesToReset = auditInfoMap.entrySet()
                        .stream()
                        .filter(entry -> Arrays.asList(PARTIALLY_COMPLETED, COMPLETED)
                                .contains(entry.getValue()
                                        .getStatus()))
                        .map(Entry::getKey)
                        .collect(Collectors.toSet());

                resetAuditInfo(auditInfoMap, tablesToReset);

            }

            // check any new table which was not there in audit info, reset audit info for them, marking PENDING
            Set<String> newTables = tables.stream()
                    .filter(table -> !auditInfoMap.containsKey(table))
                    .collect(Collectors.toSet());
            resetAuditInfo(auditInfoMap, newTables);

            return auditInfoMap.entrySet()
                    .stream()
                    .filter(entry -> PENDING.equals(entry.getValue()
                            .getStatus()))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
        };
    }

    private void resetAuditInfo(Map<String, CardinalityCalculationAuditInfo> auditInfoMap,
                                Set<String> tablesToReset) {
        if (tablesToReset.isEmpty()) {
            return;
        }
        tablesToReset.forEach(table -> {
            CardinalityCalculationAuditInfo auditInfo = CardinalityCalculationAuditInfo.builder()
                    .status(PENDING)
                    .updatedAt(new Date())
                    .build();
            auditInfoMap.put(table, auditInfo);
        });
        List<Map<String, CardinalityCalculationAuditInfo>> listOfAuditInfos = auditInfoMap.entrySet()
                .stream()
                .filter(auditInfoEntry -> PENDING.equals(auditInfoEntry.getValue()
                        .getStatus()))
                .collect(mapSize(BULK_UPDATE_CHUNK_SIZE));

        for (Map<String, CardinalityCalculationAuditInfo> cardinalityAuditInfo : listOfAuditInfos) {
            cardinalityCalculationService.updateAuditInfo(cardinalityAuditInfo);
        }

    }

    private int getMaxTimeToRunJob(int maxTimeToRunJobInMinutes) {
        if (maxTimeToRunJobInMinutes != 0) {
            return maxTimeToRunJobInMinutes;
        }
        return cardinalityConfig.getMaxTimeToRunJobInMinutes() != 0
                ? cardinalityConfig.getMaxTimeToRunJobInMinutes()
                : MAX_TIME_TO_RUN_JOB;
    }

    private int getRetryCount(int retryCount) {
        return retryCount != 0
                ? retryCount
                : cardinalityConfig.getRetryCount();

    }
}
