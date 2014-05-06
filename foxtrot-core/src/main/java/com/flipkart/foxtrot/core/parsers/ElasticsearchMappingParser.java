package com.flipkart.foxtrot.core.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class ElasticsearchMappingParser {

    private ObjectMapper mapper;

    public ElasticsearchMappingParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Set<FieldTypeMapping> getFieldMappings(MappingMetaData metaData) throws IOException {
        JsonNode jsonNode = mapper.valueToTree(metaData.getSourceAsMap());
        return generateFieldMappings(null, jsonNode.get("properties"));
    }

    private Set<FieldTypeMapping> generateFieldMappings(String parentField, JsonNode jsonNode) {
        Set<FieldTypeMapping> fieldTypeMappings = new HashSet<FieldTypeMapping>();
        Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String currentField = (parentField == null) ? entry.getKey() : (String.format("%s.%s", parentField, entry.getKey()));
            if (entry.getValue().has("properties")) {
                fieldTypeMappings.addAll(generateFieldMappings(currentField, entry.getValue().get("properties")));
            } else {
                FieldType fieldType = getFieldType(entry.getValue().get("type"));
                fieldTypeMappings.add(new FieldTypeMapping(currentField, fieldType));
            }
        }
        return fieldTypeMappings;
    }

    private FieldType getFieldType(JsonNode jsonNode) {
        String type = jsonNode.asText();
        return FieldType.valueOf(type.toUpperCase());
    }
}
