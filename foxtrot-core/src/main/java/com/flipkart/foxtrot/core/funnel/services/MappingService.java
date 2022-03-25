package com.flipkart.foxtrot.core.funnel.services;


import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import lombok.Builder;
import lombok.Data;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Singleton
@SuppressWarnings("squid:CallToDeprecatedMethod")
public class MappingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappingService.class);
    private static final String TYPE = "funnel_data";
    private static final String INDEX_TYPE = "type";
    private static final String TEXT = "text";
    private static final String PROPERTIES = "properties";
    private static final String KEYWORD = "keyword";
    private static final String DOT = ".";
    private final ElasticsearchConnection elasticsearchConnection;
    private final FunnelConfiguration funnelConfiguration;
    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings;

    @Inject
    public MappingService(final ElasticsearchConnection elasticsearchConnection,
                          final FunnelConfiguration funnelConfiguration) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.funnelConfiguration = funnelConfiguration;
    }

    private Map<String, Object> getMapping(String index) {
        if (allMappings == null || allMappings.isEmpty()) {
            allMappings = getAllMapping(elasticsearchConnection);
        }
        return allMappings.get(index)
                .get(TYPE)
                .sourceAsMap();
    }

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getAllMapping(ElasticsearchConnection elasticsearchConnection) {
        GetMappingsRequest request = new GetMappingsRequest().indices(funnelConfiguration.getFunnelIndex())
                .types(TYPE);
        try {

            GetMappingsResponse getMappingsResponse = elasticsearchConnection.getClient()
                    .indices()
                    .getMapping(request, RequestOptions.DEFAULT);
            return getMappingsResponse.mappings();
        } catch (IOException e) {
            throw new FunnelException(ErrorCode.FUNNEL_EXCEPTION, e);
        }
    }

    @SuppressWarnings("unchecked")
    public String getFieldType(String fieldName,
                               String index) {
        String defaultFieldType = TEXT;

        MappingAnalysisResult mappingAnalysisResult = analyze(fieldName, index);

        if (!mappingAnalysisResult.isFieldPresent()) {
            return defaultFieldType;
        }
        if (mappingAnalysisResult.getIndexMapping()
                .containsKey(INDEX_TYPE)) {
            return mappingAnalysisResult.getIndexMapping()
                    .get(INDEX_TYPE)
                    .toString();
        } else {
            LOGGER.info("Field does not have type in the mapping: {}", fieldName);
        }
        return defaultFieldType;
    }

    @SuppressWarnings("unchecked")
    public String getAnalyzedFieldName(String fieldName,
                                       String index) {
        String defaultFieldName = "id" + DOT + TEXT;

        MappingAnalysisResult mappingAnalysisResult = analyze(fieldName, index);

        if (!mappingAnalysisResult.isFieldPresent()) {
            return fieldName + DOT + TEXT;
        }

        if (mappingAnalysisResult.getIndexMapping()
                .containsKey(INDEX_TYPE)) {
            if (mappingAnalysisResult.getIndexMapping()
                    .get(INDEX_TYPE)
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

    private MappingAnalysisResult analyze(String fieldName,
                                          String index) {
        String[] fieldArray = fieldName.split("\\.", 5);
        if (fieldArray.length == 0) {
            fieldArray = new String[]{fieldName};
        }
        Object value;

        Map<String, Object> indexMapping = getMapping(index);
        boolean fieldPresent = true;
        for (String field : CollectionUtils.nullAndEmptySafeValueList(fieldArray)) {
            value = indexMapping.get(PROPERTIES);
            indexMapping = JsonUtils.readMapFromObject(value);
            if (indexMapping.containsKey(field)) {
                value = indexMapping.get(field);
            } else {
                LOGGER.info("Field not present: {}", field);
                fieldPresent = false;
                break;
            }
            indexMapping = JsonUtils.readMapFromObject(value);
        }

        return MappingAnalysisResult.builder()
                .fieldPresent(fieldPresent)
                .indexMapping(indexMapping)
                .build();
    }

    @Data
    @Builder
    public static class MappingAnalysisResult {

        private boolean fieldPresent;
        private Map<String, Object> indexMapping;
    }
}
