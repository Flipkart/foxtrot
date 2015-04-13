package com.flipkart.foxtrot.sql.responseprocessors;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.flipkart.foxtrot.sql.responseprocessors.model.MetaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

public class FlatteningUtils {
    private static final String DEFAULT_SEPARATOR = ".";

    public static FlatRepresentation genericParse(JsonNode response) {
        List<FieldHeader> headers = Lists.newArrayList();
        Map<String, MetaData> docFields = generateFieldMappings(null, response);
        Map<String, Object> row = Maps.newTreeMap();
        for(Map.Entry<String, MetaData> docField : docFields.entrySet()) {
            row.put(docField.getKey(), docField.getValue().getData());
            headers.add(new FieldHeader(docField.getKey(), 20));
        }
        return new FlatRepresentation(headers, Collections.singletonList(row));
    }

    public static FlatRepresentation genericMultiRowParse(JsonNode response, final List<String> predefinedHeaders, final String sortField) {
        List<FieldHeader> headers = Lists.newArrayList();
        List<Map<String, Object>> rows = Lists.newArrayList();
        Map<String, Integer> headerData = Maps.newTreeMap();

        for(JsonNode arrayElement : response) {
            //Map<String, MetaData> tmpFields = generateFieldMappings(parentField, arrayElement);
            //fields.putAll(tmpFields);
            Map<String, MetaData> element = generateFieldMappings(null, arrayElement);
            Map<String, Object> row = Maps.newHashMap();
            for(Map.Entry<String, MetaData> elementData : element.entrySet()) {
                if(!headerData.containsKey(elementData.getKey())) {
                    headerData.put(elementData.getKey(), elementData.getValue().getLength());
                }
                headerData.put(elementData.getKey(), Math.max(elementData.getValue().getLength(), headerData.get(elementData.getKey())));
                row.put(elementData.getKey(), elementData.getValue().getData());
            }
            rows.add(row);
        }
        if(null != sortField && !sortField.isEmpty()) {
            Collections.sort(rows, new Comparator<Map<String, Object>>() {
                @Override
                public int compare(Map<String, Object> lhs, Map<String, Object> rhs) {
                    return lhs.get(sortField).toString().compareTo(rhs.get(sortField).toString());
                }
            });
        }

        if(null != predefinedHeaders && !predefinedHeaders.isEmpty()) {
            for (String predefinedHeader : predefinedHeaders) {
                if (headerData.containsKey(predefinedHeader)) {
                    headers.add(new FieldHeader(predefinedHeader, headerData.get(predefinedHeader)));
                }
            }
        }
        else {
            for (String definedHeaders : headerData.keySet()) {
                if (headerData.containsKey(definedHeaders)) {
                    headers.add(new FieldHeader(definedHeaders, headerData.get(definedHeaders)));
                }
            }
        }
        return new FlatRepresentation(headers, rows);
    }

    public static Map<String, MetaData> generateFieldMappings(String parentField, JsonNode jsonNode) {
        return generateFieldMappings(parentField, jsonNode, DEFAULT_SEPARATOR);
    }

    public static Map<String, MetaData> generateFieldMappings(String parentField, JsonNode jsonNode, final String separator) {
        Map<String, MetaData> fields = Maps.newTreeMap();
        if(null == jsonNode) {
            System.out.println("NULL for " + parentField);
            return Collections.emptyMap();
        }
        if(jsonNode.isArray()) {
            for(JsonNode arrayElement : jsonNode) {
                Map<String, MetaData> tmpFields = generateFieldMappings(parentField, arrayElement, separator);
                fields.putAll(tmpFields);
            }
        }
        Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            String currentField = (parentField == null) ? entry.getKey() : (String.format("%s%s%s", parentField, separator, entry.getKey()));
            if (entry.getValue().isObject() || entry.getValue().isArray()) {
                fields.putAll(generateFieldMappings(currentField, entry.getValue(), separator));
            } else {
                fields.put(currentField, new MetaData(entry.getValue(), entry.getValue().toString().length()));
            }
        }
        return fields;
    }

}
