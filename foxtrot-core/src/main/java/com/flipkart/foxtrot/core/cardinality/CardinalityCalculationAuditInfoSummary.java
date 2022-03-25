package com.flipkart.foxtrot.core.cardinality;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CardinalityCalculationAuditInfoSummary {

    private CountSummary count;
    private TimeTakenSummary timeTaken;
    private Map<String, CardinalityCalculationAuditInfo> auditInfo;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CountSummary {

        private long total;
        private Map<String, Long> status;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TimeTakenSummary {

        private double average;
        private double p999;
        private double p99;
        private double p95;
        private double p75;
        private double p50;
    }
}
