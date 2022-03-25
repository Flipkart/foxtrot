package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.cardinality.CardinalityCalculationAuditInfo.CardinalityStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CardinalityCalculationResult {

    private TableFieldMapping tableFieldMapping;

    private List<Exception> errors;

    public boolean isEvenPartiallySuccessful() {
        return Objects.nonNull(tableFieldMapping) && !CollectionUtils.isNullOrEmpty(tableFieldMapping.getMappings())
                && tableFieldMapping.getMappings()
                .stream()
                .anyMatch(fieldMetadata -> fieldMetadata.getEstimationData() != null);
    }

    public CardinalityStatus getStatus() {
        if (!isEvenPartiallySuccessful()) {
            return CardinalityStatus.FAILED;
        } else if (!CollectionUtils.isNullOrEmpty(this.errors)) {
            return CardinalityStatus.PARTIALLY_COMPLETED;
        } else {
            return CardinalityStatus.COMPLETED;
        }
    }
}
