package com.flipkart.foxtrot.core.cardinality;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CardinalityCalculationAuditInfo {

    public static final String STATUS_ATTRIBUTE = "status";
    public static final String TIME_TAKEN_ATTRIBUTE = "timeTakenInMillis";
    private Date updatedAt;
    private CardinalityStatus status;
    private long timeTakenInMillis;

    public enum CardinalityStatus {
        PENDING,
        PARTIALLY_COMPLETED,
        COMPLETED,
        FAILED
    }

}
