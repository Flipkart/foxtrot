package com.flipkart.foxtrot.core.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.FieldDataType;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import lombok.Builder;
import lombok.Data;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.*;

@Singleton
public class ElasticsearchTemplateMappingParser {

    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String FIELDS = "fields";
    private static final String ANALYZED = "analyzed";


    public PutIndexTemplateRequest buildIndexMappingTemplateRequest(final Table table) {
        PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest().name(
                getMappingTemplateName(table.getName()))
                .patterns(Lists.newArrayList(String.format("%s*", getIndexPrefix(table.getName()))))
                .order(1)
                .settings(Settings.builder()
                        .put("index.number_of_shards", table.getShards())
                        .put("index.refresh_interval", table.getRefreshIntervalInSecs() == 0
                                ? 30
                                : table.getRefreshIntervalInSecs(), TimeUnit.SECONDS)
                        .put("index.mapping.total_fields.limit", table.getColumns())
                        .build());
        if (Objects.nonNull(table.getCustomFieldMappings()) && !table.getCustomFieldMappings()
                .isEmpty()) {
            putIndexTemplateRequest.mapping(DOCUMENT_TYPE_NAME, getDocumentMapping(table.getCustomFieldMappings()));
        }
        return putIndexTemplateRequest;
    }

    private Map<String, Object> getDocumentMapping(SortedMap<String, FieldDataType> customFieldMappings) {
        if (customFieldMappings.isEmpty()) {
            return new HashMap<>();
        }
        ObjectNode rootNode = JsonUtils.createObjectNode();

        // parent json node to start with
        ObjectNode objectNode = JsonUtils.createObjectNode();

        // for every column we traverse in the parent json node from starting
        // maintain following constraints :
        // 1.
        for (Entry<String, FieldDataType> entry : customFieldMappings.entrySet()) {

            // split the dot separated column name, so that we can traverse in the json for each token
            String[] tokens = entry.getKey()
                    .split("\\.");

            // initialize traversal context with pointer pointing to starting of parent json
            // set oldPointerPropertiesNode = false
            TraversalContext traversalContext = TraversalContext.builder()
                    .oldPointerPropertiesNode(false)
                    .pointer(objectNode)
                    .build();

            // The invariant we will be maintaining is that the pointer to json will always point to
            // A. properties json or B. non properties node
            // when we reach the last token we'll populate the mapping type json
            for (int i = 0; i < tokens.length; i++) {
                // if the json at current pointer contains this token we handle the match case
                if (traversalContext.getPointer()
                        .has(tokens[i])) {
                    handleMatchCase(tokens[i], traversalContext);
                } else {
                    // handle non match case
                    handleNonMatchCase(objectNode, entry, tokens, traversalContext, i);
                }
            }
        }

        ObjectNode propertiesNode = JsonUtils.createObjectNode();
        propertiesNode.set(PROPERTIES, objectNode);
        rootNode.set(DOCUMENT_TYPE_NAME, propertiesNode);

        return JsonUtils.readMapFromObject(rootNode);
    }

    private void handleMatchCase(String token,
                                 TraversalContext traversalContext) {
        // for match case we try to deeper inside the properties json if it's present
        // if there's no properties json inside current json we set oldPointerPropertiesNode flag as false
        traversalContext.setPointer(traversalContext.getPointer()
                .get(token));
        if (traversalContext.getPointer()
                .has(PROPERTIES)) {
            traversalContext.setPointer(traversalContext.getPointer()
                    .get(PROPERTIES));
            traversalContext.setOldPointerPropertiesNode(true);
        } else {
            traversalContext.setOldPointerPropertiesNode(false);
        }
    }

    private void handleNonMatchCase(ObjectNode objectNode,
                                    Entry<String, FieldDataType> entry,
                                    String[] tokens,
                                    TraversalContext traversalContext,
                                    int i) {
        if (i == tokens.length - 1) {
            handleLastToken(objectNode, entry, tokens, traversalContext);
        } else {
            ObjectNode node = (ObjectNode) traversalContext.getPointer();
            if (traversalContext.isOldPointerPropertiesNode()) {
                handlePropertiesNode(tokens[i], traversalContext, node);
            } else {
                handleNonPropertiesNode(objectNode, tokens, traversalContext, i, node);
            }

        }
    }

    private void handlePropertiesNode(String token,
                                      TraversalContext traversalContext,
                                      ObjectNode node) {
        ObjectNode properties = JsonUtils.createObjectNode();
        ObjectNode newNode = JsonUtils.createObjectNode();
        properties.set(PROPERTIES, newNode);
        node.set(token, properties);
        traversalContext.setPointer(newNode);
    }

    private void handleNonPropertiesNode(ObjectNode objectNode,
                                         String[] tokens,
                                         TraversalContext traversalContext,
                                         int i,
                                         ObjectNode node) {
        if (traversalContext.getPointer() != objectNode) {
            ObjectNode newNode = JsonUtils.createObjectNode();
            ObjectNode properties = JsonUtils.createObjectNode();
            properties.set(PROPERTIES, newNode);

            ObjectNode childNode = JsonUtils.createObjectNode();
            childNode.set(tokens[i], properties);

            node.set(PROPERTIES, childNode);
            traversalContext.setOldPointerPropertiesNode(true);
            traversalContext.setPointer(newNode);
        } else {
            ObjectNode newNode = JsonUtils.createObjectNode();
            ObjectNode properties = JsonUtils.createObjectNode();
            properties.set(PROPERTIES, newNode);

            node.set(tokens[i], properties);
            traversalContext.setOldPointerPropertiesNode(true);
            traversalContext.setPointer(newNode);
        }
    }

    private void handleLastToken(ObjectNode objectNode,
                                 Entry<String, FieldDataType> entry,
                                 String[] tokens,
                                 TraversalContext traversalContext) {
        if (entry.getValue()
                .mapping() == null) {
            return;
        }

        // if last node was properties node we just need to populate - mapping type
        if (traversalContext.isOldPointerPropertiesNode()) {
            ObjectNode node = (ObjectNode) traversalContext.getPointer();
            node.set(tokens[tokens.length - 1], entry.getValue()
                    .mapping());
        } else {
            // if last node was not properties node :
            // if pointer is not at parent json , we need to populate mapping type inside the properties node
            // else if pointer is at parent json then we just need to populate mapping type
            if (traversalContext.getPointer() != objectNode) {
                ObjectNode node = (ObjectNode) traversalContext.getPointer();
                ObjectNode newNode = JsonUtils.createObjectNode();
                newNode.set(tokens[tokens.length - 1], entry.getValue()
                        .mapping());
                node.set(PROPERTIES, newNode);
            } else {
                ObjectNode node = (ObjectNode) traversalContext.getPointer();
                node.set(tokens[tokens.length - 1], entry.getValue()
                        .mapping());
            }
        }
    }

    public Map<String, FieldDataType> getFieldMappings(MappingMetaData metaData) {
        JsonNode jsonNode = JsonUtils.toJsonNode(metaData.getSourceAsMap());
        return generateFieldMappings(null, jsonNode.get(PROPERTIES));
    }

    private Map<String, FieldDataType> generateFieldMappings(String parentField,
                                                             JsonNode jsonNode) {
        Map<String, FieldDataType> fieldTypeMappings = new HashMap<>();
        Iterator<Entry<String, JsonNode>> iterator = jsonNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            if (entry.getKey()
                    .startsWith(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME)) {
                continue;
            }
            String currentField = (parentField == null)
                    ? entry.getKey()
                    : (String.format("%s.%s", parentField, entry.getKey()));
            JsonNode node = entry.getValue();
            if (node.has(PROPERTIES) && node.has(TYPE)) {
                FieldDataType fieldType = getFieldDataType(node.get(TYPE));
                fieldTypeMappings.put(currentField, fieldType);
                fieldTypeMappings.putAll(generateFieldMappings(currentField, node.get(PROPERTIES)));
            } else if (node.has(PROPERTIES)) {
                fieldTypeMappings.putAll(generateFieldMappings(currentField, node.get(PROPERTIES)));
            } else {
                node = handleAnalyzedFields(node);
                FieldDataType fieldType = getFieldDataType(node.get(TYPE));
                fieldTypeMappings.put(currentField, fieldType);
            }
        }
        return fieldTypeMappings;
    }

    private JsonNode handleAnalyzedFields(JsonNode node) {
        if (node.has(FIELDS)) {
            node = node.get(FIELDS);
            if (node.has(ANALYZED)) {
                node = node.get(ANALYZED);
            }
        }
        return node;
    }

    private FieldDataType getFieldDataType(JsonNode jsonNode) {
        String type = jsonNode.asText();
        return FieldDataType.valueOf(type.toUpperCase());
    }

    @Data
    @Builder
    private static final class TraversalContext {

        private JsonNode pointer;

        // boolean flag to keep track of whether the old jsonNode pointer was properties node
        private boolean oldPointerPropertiesNode;
    }
}
