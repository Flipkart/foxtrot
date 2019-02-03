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
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import net.javacrumbs.shedlock.core.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/***
 Created by nitish.goyal on 13/08/18
 ***/

public class CardinalityCalculationRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CardinalityCalculationRunnable.class.getSimpleName());
    private final TableMetadataManager tableMetadataManager;

    public CardinalityCalculationRunnable(TableMetadataManager tableMetadataManager) {
        this.tableMetadataManager = tableMetadataManager;
    }

    @SchedulerLock(name = "cardinalityCalculation")
    @Override
    public void run() {
        try {
            Set<String> tables = tableMetadataManager.get().stream().map(Table::getName).collect(Collectors.toSet());
            for (String table : tables) {
                tableMetadataManager.getFieldMappings(table, true, true);
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while calculating cardinality " + e);
        }
    }
}
