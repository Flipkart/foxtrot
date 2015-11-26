/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.FieldType;
import com.flipkart.foxtrot.common.FieldTypeMapping;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
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
            if(entry.getKey().equals(ElasticsearchUtils.DOCUMENT_META_FIELD_NAME)) {
                continue;
            }
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
