package com.flipkart.foxtrot.pipeline.processors.geo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.geojson.Feature;
import org.geojson.FeatureCollection;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GeoJsonReaderImpl implements GeoJsonReader {

    private ObjectMapper objectMapper;

    @Inject
    public GeoJsonReaderImpl(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public List<Feature> fetchFeatures(String geoJsonSource) throws IOException {
        URI uri = URI.create(geoJsonSource);
        switch (uri.getScheme()) {
            case "res":
                URL url = Resources.getResource(uri.getHost());
                String resourceData = Resources.toString(url, StandardCharsets.UTF_8);
                return getFeatures(resourceData);
            case "http":
                String httpData = IOUtils.toString(uri, StandardCharsets.UTF_8);
                return getFeatures(httpData);
            default:
                throw new UnsupportedOperationException(String.format("Scheme [%s] not supported", uri.getScheme()));
        }
    }

    @Override
    public List<Feature> fetchFeatures(String sourceURI,
                                       Set<String> ids) throws IOException {
        return fetchFeatures(sourceURI).stream()
                .filter(feature -> ids == null || ids.contains(feature.getId()))
                .collect(Collectors.toList());
    }

    private List<Feature> getFeatures(String text) throws IOException {
        val feature = objectMapper.readValue(text, FeatureCollection.class);
        return feature.getFeatures();
    }
}
