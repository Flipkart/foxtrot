package com.flipkart.foxtrot.core.querystore.mutator;

import com.fasterxml.jackson.databind.JsonNode;

public interface IndexerEventMutator {

    void mutate(String table,
                String documentId,
                JsonNode node);

}
