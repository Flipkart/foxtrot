/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.FieldMetadata;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by rishabh.goyal on 06/05/14.
 */
public class ElasticsearchMappingParser {

    private static final String PROPERTIES = "properties";
    private ObjectMapper mapper;

    public ElasticsearchMappingParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Set<FieldMetadata> getFieldMappings(MappingMetaData metaData) {
        JsonNode jsonNode = mapper.valueToTree(metaData.getSourceAsMap());
        return generateFieldMappings(null, jsonNode.get(PROPERTIES));
    }

    private Set<FieldMetadata> generateFieldMappings(String parentField, JsonNode jsonNode) {
        Set<FieldMetadata> fieldTypeMappings = new HashSet<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            if (entry.getKey()
                    .startsWith(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME)) {
                continue;
            }
            String currentField = (parentField == null) ? entry.getKey() : (String.format("%s.%s", parentField, entry.getKey()));
            if (entry.getValue()
                    .has(PROPERTIES)) {
                fieldTypeMappings.addAll(generateFieldMappings(currentField, entry.getValue()
                        .get(PROPERTIES)));
            } else {
                FieldType fieldType = getFieldType(entry.getValue()
                        .get("type"));
                fieldTypeMappings.add(FieldMetadata.builder()
                        .field(currentField)
                        .type(fieldType)
                        .build());
            }
        }
        return fieldTypeMappings;
    }

    private FieldType getFieldType(JsonNode jsonNode) {
        String type = jsonNode.asText();
        FieldType fieldType = FieldType.valueOf(type.toUpperCase());
        if (fieldType == FieldType.TEXT || fieldType == FieldType.KEYWORD) {
            return FieldType.STRING;
        }
        return fieldType;
    }
}
