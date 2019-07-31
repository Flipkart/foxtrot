package com.flipkart.foxtrot.core.querystore.mutator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.core.config.IndexConfiguration;
import com.google.common.collect.Lists;
import lombok.val;

import java.util.List;

public class LargeTextNodeRemover implements IndexerEventMutator {

    private final ObjectMapper objectMapper;
    private final int textFieldMaxSize;

    public LargeTextNodeRemover(ObjectMapper objectMapper,
                                IndexConfiguration indexConfiguration) {
        this.objectMapper = objectMapper;
        this.textFieldMaxSize = indexConfiguration.getTextFieldMaxSize();
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
        List<String> toBeRemoved = Lists.newArrayList();
        objectNode.fields().forEachRemaining(entry -> {
            val value = entry.getValue();
            if (value.isTextual()) {
                if(!value.isNull() && value.textValue().length() > textFieldMaxSize){
                    toBeRemoved.add(entry.getKey());
                }
            } else if (value.isArray()) {
                handleArrayNode((ArrayNode) value);
            } else if (value.isObject()) {
                handleObjectNode((ObjectNode) value);
            }
        });
        objectNode.remove(toBeRemoved);
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
                        copyNode = node.isNull() || node.textValue().length() <= textFieldMaxSize;
                    }
                    if (copyNode) {
                        copy.add(node);
                    }
                });
        arrayNode.removeAll();
        arrayNode.addAll(copy);
    }
}
