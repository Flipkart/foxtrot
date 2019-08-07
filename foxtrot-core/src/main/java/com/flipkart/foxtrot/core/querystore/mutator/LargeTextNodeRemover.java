package com.flipkart.foxtrot.core.querystore.mutator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Random;

@Slf4j
public class LargeTextNodeRemover implements IndexerEventMutator {

    private final ObjectMapper objectMapper;
    private final TextNodeRemoverConfiguration configuration;
    private final Random random;

    public LargeTextNodeRemover(ObjectMapper objectMapper,
                                TextNodeRemoverConfiguration textNodeRemoverConfiguration) {
        this.objectMapper = objectMapper;
        this.configuration = textNodeRemoverConfiguration;
        this.random = new Random();
    }


    @Override
    public void mutate(String table, JsonNode data) {
        walkTree(table, data);
    }


    private void walkTree(String table,
                          JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            handleObjectNode(table, (ObjectNode) node);
        } else if (node.isArray()) {
            handleArrayNode(table, null, (ArrayNode) node);
        }
    }

    private void handleObjectNode(final String table,
                                  ObjectNode objectNode) {
        if (objectNode == null || objectNode.isNull()) {
            return;
        }
        List<String> toBeRemoved = Lists.newArrayList();
        objectNode.fields().forEachRemaining(entry -> {
            val key = entry.getKey();
            val value = entry.getValue();
            if (value.isTextual()) {
                boolean removeEntry = evaluateForRemoval(table, key, value);
                if (removeEntry) {
                    toBeRemoved.add(entry.getKey());
                }
            } else if (value.isArray()) {
                handleArrayNode(table, key, (ArrayNode) value);
            } else if (value.isObject()) {
                handleObjectNode(table, (ObjectNode) value);
            }
        });
        objectNode.remove(toBeRemoved);
    }

    private void handleArrayNode(final String table,
                                 final String parentKey,
                                 ArrayNode arrayNode) {
        if (arrayNode == null || arrayNode.isNull()) {
            return;
        }
        ArrayNode copy = objectMapper.createArrayNode();
        arrayNode.elements()
                .forEachRemaining(node -> {
                    boolean copyNode = true;
                    if (node.isObject()) {
                        handleObjectNode(table, (ObjectNode) node);
                    } else if (node.isArray()) {
                        handleArrayNode(table, parentKey, (ArrayNode) node);
                    } else if (node.isTextual()) {
                        copyNode = !evaluateForRemoval(table, parentKey, node);
                    }
                    if (copyNode) {
                        copy.add(node);
                    }
                });
        arrayNode.removeAll();
        arrayNode.addAll(copy);
    }

    private boolean evaluateForRemoval(final String table,
                                       final String key,
                                       JsonNode node) {
        if (!node.isTextual()) {
            return false;
        }

        if (node.isNull()) {
            return false;
        }

        if (node.textValue().length() < configuration.getMaxAllowedSize()) {
            return false;
        }

        if (random.nextInt(100) < configuration.getLogSamplingPercentage()) {
            log.warn("LargeTextNodeDetected table: {} key: {} size:{} value: {}",
                    table, key, node.textValue().length(), node.textValue());
        }

        return random.nextInt(100) < configuration.getBlockPercentage();
    }
}
