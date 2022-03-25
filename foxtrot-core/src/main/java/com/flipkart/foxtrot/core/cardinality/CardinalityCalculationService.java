package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.TableFieldMapping;

import java.util.Map;

public interface CardinalityCalculationService {

    CardinalityCalculationResult calculateCardinality(TableFieldMapping tableFieldMapping);

    void updateAuditInfo(String table,
                         CardinalityCalculationAuditInfo auditInfo);

    void updateAuditInfo(Map<String, CardinalityCalculationAuditInfo> calculationAuditInfoMap);

    CardinalityCalculationAuditInfoSummary fetchAuditSummary(boolean detailed);

    Map<String, CardinalityCalculationAuditInfo> fetchAuditInfo();
}
