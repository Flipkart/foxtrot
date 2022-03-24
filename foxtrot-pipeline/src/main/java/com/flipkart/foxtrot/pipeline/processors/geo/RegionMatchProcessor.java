package com.flipkart.foxtrot.pipeline.processors.geo;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.pipeline.JsonPathUtils;
import com.flipkart.foxtrot.pipeline.ProcessorMarker;
import com.flipkart.foxtrot.pipeline.geo.GeoRegionIndex;
import com.flipkart.foxtrot.pipeline.geo.PointExtractor;
import com.flipkart.foxtrot.pipeline.geo.S2CachedGeoRegionIndex;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorInitializationException;
import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReader;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.jayway.jsonpath.JsonPath;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map.Entry;

@Slf4j
@Setter
@ProcessorMarker(name = "GEO::REGION_MATCHER", processorClass = RegionMatchProcessorDefinition.class)
@NoArgsConstructor
@AllArgsConstructor
public class RegionMatchProcessor implements Processor<RegionMatchProcessorDefinition> {

    private RegionMatchProcessorDefinition processorDefinition;

    @Inject
    private GeoRegionIndex regionIndex;
    @Inject
    private GeoJsonReader geoJsonReader;
    @Inject
    private PointExtractor pointExtractor;

    private boolean initialized;

    @Override
    public void init() throws ProcessorInitializationException {
        if (initialized) {
            return;
        }
        try {
            if (!Strings.isNullOrEmpty(processorDefinition.getCacheSpec())) {
                regionIndex = new S2CachedGeoRegionIndex(regionIndex, processorDefinition.getCacheSpec(),
                        processorDefinition.getCacheS2Level());
            }
            regionIndex.index(geoJsonReader.fetchFeatures(processorDefinition.getGeoJsonSource()));
            initialized = true;
        } catch (Exception e) {
            throw new ProcessorInitializationException("Error Initializing RegionMatchProcesor", e);
        }
    }

    @Override
    @MonitoredFunction
    public void handle(Document document) {
        String matchField = processorDefinition.getMatchField();
        val docContext = JsonPath.parse(document.getData());

        val locationRootNodeOptional = JsonPathUtils.readPath(docContext, matchField);
        val optionalPoint = locationRootNodeOptional.flatMap(locationRootNode -> pointExtractor.extractFromRoot(locationRootNode));
        if (!optionalPoint.isPresent()) {
            log.debug("Point not present in document " + document.getId());
            return;
        }
        val matchingFeatures = regionIndex.matchingIds(optionalPoint.get());

        if (matchingFeatures.size() > 0) {
            val feature = matchingFeatures.get(0);
            for (Entry<String, String> targetPropertyEntry : processorDefinition.getTargetFieldRootMapping()
                    .entrySet()) {
                JsonPathUtils.create(docContext, targetPropertyEntry.getKey(), feature.getProperties()
                        .get(targetPropertyEntry.getValue()));
                document.setData(docContext.json());
            }
        }
    }
}




