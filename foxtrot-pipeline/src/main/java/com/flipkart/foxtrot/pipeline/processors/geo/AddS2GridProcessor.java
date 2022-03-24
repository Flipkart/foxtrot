package com.flipkart.foxtrot.pipeline.processors.geo;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.pipeline.JsonPathUtils;
import com.flipkart.foxtrot.pipeline.ProcessorMarker;
import com.flipkart.foxtrot.pipeline.geo.GeoUtils;
import com.flipkart.foxtrot.pipeline.geo.PointExtractor;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorExecutionException;
import com.flipkart.foxtrot.pipeline.processors.TargetWriteMode;
import com.google.common.geometry.S2CellId;
import com.google.inject.Inject;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Setter
@ProcessorMarker(name = "GEO::S2_GRID", processorClass = AddS2GridProcessorDefinition.class)
@AllArgsConstructor
@NoArgsConstructor
public class AddS2GridProcessor implements Processor<AddS2GridProcessorDefinition> {

    private AddS2GridProcessorDefinition processorDefinition;
    @Inject
    private PointExtractor pointExtractor;

    @Override
    @MonitoredFunction
    public void handle(Document document) {
        String matchField = processorDefinition.getMatchField();
        val docContext = JsonPath.parse(document.getData());
        try {
            val locationRootNodeOptional = JsonPathUtils.readPath(docContext, matchField);
            val optionalPoint = locationRootNodeOptional.flatMap(
                    locationRootNode -> pointExtractor.extractFromRoot(locationRootNode));
            if (!optionalPoint.isPresent()) {
                log.debug("Point not present in document id:[{}] matchField:[{}] json:[{}]", document.getId(),
                        matchField, JsonUtils.toString(document.getData()));
                return;
            }
            val id = S2CellId.fromLatLng(GeoUtils.toS2LatLng(optionalPoint.get()));
            for (Integer level : processorDefinition.getS2Levels()) {
                String targetPath = processorDefinition.getTargetFieldRoot() + "." + level;
                if (processorDefinition.getTargetWriteMode() == TargetWriteMode.CREATE_ONLY && pathExists(docContext,
                        targetPath)) {
                    throw new ProcessorExecutionException("Target Path already exists and write mode is CREATE_ONLY",
                            null);
                }
                JsonPathUtils.create(docContext, targetPath, id.parent(level)
                        .toToken());
            }
        } catch (Exception e) {
            throw new ProcessorExecutionException("Error Executing AddS2GridProcessor", e);
        }
    }

    private boolean pathExists(DocumentContext context,
                               String s) {
        try {
            context.read(s);
        } catch (PathNotFoundException e) {
            return false;
        }
        return true;
    }


}




