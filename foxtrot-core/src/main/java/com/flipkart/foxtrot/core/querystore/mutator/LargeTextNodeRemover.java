package com.flipkart.foxtrot.core.querystore.mutator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LargeTextNodeRemover implements IndexerEventMutator {

    private static final int MAX_SIZE = 500;
    private final ObjectMapper objectMapper;

    public LargeTextNodeRemover(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public void mutate(String table, JsonNode data) {
        walkTree(data);
    }


    private void walkTree(JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            handleObjectNode((ObjectNode) node);
        } else if (node.isArray()) {
            handleArrayNode((ArrayNode) node);
        }
    }

    private void handleObjectNode(ObjectNode objectNode) {
        if (objectNode == null || objectNode.isNull()) {
            return;
        }
        objectNode.fields().forEachRemaining(entry -> {
            if (entry.getValue().isTextual()) {
                if (entry.getValue().size() > MAX_SIZE) {
                    objectNode.remove(entry.getKey());
                }
            } else if (entry.getValue().isArray()) {
                handleArrayNode((ArrayNode) entry.getValue());
            } else if (entry.getValue().isObject()) {
                handleObjectNode((ObjectNode) entry.getValue());
            }
        });
    }

    private void handleArrayNode(ArrayNode arrayNode) {
        if (arrayNode == null || arrayNode.isNull()) {
            return;
        }
        ArrayNode copy = objectMapper.createArrayNode();
        arrayNode.elements()
                .forEachRemaining(node -> {
                    boolean copyNode = true;
                    if (node.isObject()) {
                        handleObjectNode((ObjectNode) node);
                    } else if (node.isArray()) {
                        handleArrayNode((ArrayNode) node);
                    } else if (node.isTextual()) {
                        copyNode = node.size() <= MAX_SIZE;
                    }
                    if (copyNode) {
                        copy.add(node);
                    }
                });
        arrayNode.removeAll();
        arrayNode.addAll(copy);
    }
}
