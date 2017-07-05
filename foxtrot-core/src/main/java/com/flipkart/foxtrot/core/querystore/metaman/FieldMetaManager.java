package com.flipkart.foxtrot.core.querystore.metaman;

import com.flipkart.foxtrot.common.FieldCardinality;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import lombok.extern.slf4j.Slf4j;

/**
 * Estimate cardinality based on heuristic
 */
@Slf4j
public class FieldMetaManager {

    private final QueryStore queryStore;

    public FieldMetaManager(QueryStore queryStore) {
        this.queryStore = queryStore;
    }

    public void estimateCardinality(TableFieldMapping fieldMapping) {
        final String table = fieldMapping.getTable();
        fieldMapping.getMappings()
                .stream()
                .filter(fieldMetadata -> fieldMetadata.getType().equals(FieldType.STRING))
                .forEach(fieldMetadata -> enrich(table, fieldMetadata));

    }

    private void enrich(final String table, FieldMetadata metadata) {
        try {
            FieldCardinality cardinality = queryStore.estimate(table, metadata.getField());
            metadata.setEstimatedCardinalityScore(
                    100 * (cardinality.getCardinality()
                            / (0 != cardinality.getCount() ? cardinality.getCount() : 1)));
            metadata.setEstimatedCountScore(cardinality.getCount());
        } catch (FoxtrotException e) {
            log.error("Error estimating cardinality for [" + table + ": " + metadata.getField() + "]", e);
        }
    }
}
