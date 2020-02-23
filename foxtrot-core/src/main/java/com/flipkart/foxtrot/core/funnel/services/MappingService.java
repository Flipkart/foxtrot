package com.flipkart.foxtrot.core.funnel.services;

import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.FUNNEL_INDEX;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.JsonUtils;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Singleton
public class MappingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappingService.class);
    private static final String TYPE = "funnel_data";
    private static final String INDEX_TYPE = "type";
    private static final String TEXT = "text";
    private static final String PROPERTIES = "properties";
    private static final String KEYWORD = "keyword";
    private static final String DOT = ".";
    private final ElasticsearchConnection elasticsearchConnection;
    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings;

    @Inject
    public MappingService(ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
    }

    private Map<String, Object> getMapping(String index) {
        if (allMappings == null || allMappings.isEmpty()) {
            allMappings = getAllMapping(elasticsearchConnection);
        }
        return allMappings.get(index)
                .get(TYPE)
                .sourceAsMap();
    }

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getAllMapping(
            ElasticsearchConnection elasticsearchConnection) {
        GetMappingsRequest request = new GetMappingsRequest().indices(FUNNEL_INDEX)
                .types(TYPE);
        GetMappingsResponse getMappingsResponse = elasticsearchConnection.getClient()
                .admin()
                .indices()
                .getMappings(request)
                .actionGet();
        return getMappingsResponse.mappings();
    }

    @SuppressWarnings("unchecked")
    public String getFieldType(String fieldName, String index) {
        String defaultFieldType = TEXT;
        String[] fieldArray = fieldName.split("\\.", 5);
        if (fieldArray.length == 0) {
            fieldArray = new String[]{fieldName};
        }
        Object value;
        Map<String, Object> indexMapping = getMapping(index);
        for (String field : CollectionUtils.nullAndEmptySafeValueList(fieldArray)) {
            value = indexMapping.get(PROPERTIES);
            indexMapping = JsonUtils.readMapFromObject(value);
            if (indexMapping.containsKey(field)) {
                value = indexMapping.get(field);
            } else {
                LOGGER.info("Field not present: {}", field);
                return defaultFieldType;
            }
            indexMapping = JsonUtils.readMapFromObject(value);
        }
        if (indexMapping.containsKey(INDEX_TYPE)) {
            return indexMapping.get(INDEX_TYPE)
                    .toString();
        } else {
            LOGGER.info("Field does not have type in the mapping: {}", fieldName);
        }
        return defaultFieldType;
    }

    @SuppressWarnings("unchecked")
    public String getAnalyzedFieldName(String fieldName, String index) {
        String defaultFieldName = "id" + DOT + TEXT;
        String[] fieldArray = fieldName.split("\\.", 5);
        if (fieldArray.length == 0) {
            fieldArray = new String[]{fieldName};
        }
        Object value;
        Map<String, Object> indexMapping = getMapping(index);
        for (String field : CollectionUtils.nullAndEmptySafeValueList(fieldArray)) {
            value = indexMapping.get(PROPERTIES);
            indexMapping = JsonUtils.readMapFromObject(value);
            if (indexMapping.containsKey(field)) {
                value = indexMapping.get(field);
            } else {
                LOGGER.info("Field not present: {}", field);
                return fieldName + DOT + TEXT;
            }
            indexMapping = JsonUtils.readMapFromObject(value);
        }
        if (indexMapping.containsKey(INDEX_TYPE)) {
            if (indexMapping.get(INDEX_TYPE)
                    .toString()
                    .equals(KEYWORD)) {
                return fieldName + DOT + TEXT;
            }
            return fieldName;
        } else {
            LOGGER.info("Field does not have type in the mapping: {}", fieldName);
        }
        LOGGER.info("Issue in field mapping. Sorting on id");
        return defaultFieldName;
    }
}
