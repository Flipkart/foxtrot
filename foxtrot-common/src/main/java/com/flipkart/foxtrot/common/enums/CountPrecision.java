package com.flipkart.foxtrot.common.enums;

import lombok.Getter;

public enum CountPrecision {
    HIGH(40000),
    MEDIUM(3000),
    LOW(100);

    @Getter
    private Integer precisionThreshold;

    CountPrecision(Integer precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
    }
}
