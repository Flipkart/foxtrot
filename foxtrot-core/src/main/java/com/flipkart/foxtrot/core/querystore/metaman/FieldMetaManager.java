package com.flipkart.foxtrot.core.querystore.metaman;

import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

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
        try {
            queryStore.estimateCardinality(table,
            fieldMapping.getMappings()
                    .stream()
                    .filter(fieldMetadata -> fieldMetadata.getType().equals(FieldType.STRING))
                    .collect(Collectors.toList()));
        } catch (FoxtrotException e) {
            log.error("Error estimating cardinality for " + table, e);
        }

    }
}
