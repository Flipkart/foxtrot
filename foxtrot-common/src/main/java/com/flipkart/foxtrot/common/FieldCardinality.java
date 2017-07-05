package com.flipkart.foxtrot.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Cardinality and count for a field
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FieldCardinality {
    private long cardinality;
    private long count;
}
