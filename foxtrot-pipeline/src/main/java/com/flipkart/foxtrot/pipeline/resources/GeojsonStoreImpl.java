package com.flipkart.foxtrot.pipeline.resources;

import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReader;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import lombok.SneakyThrows;
import lombok.val;
import org.geojson.Feature;

import javax.ws.rs.WebApplicationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GeojsonStoreImpl implements GeojsonStore {

    private Map<String, GeojsonCollectionSource> geojsonStoreConfiguration;
    private GeoJsonReader geoJsonReader;

    @Inject
    public GeojsonStoreImpl(GeoJsonReader geoJsonReader,
                            GeojsonStoreConfiguration geojsonStoreConfiguration) {
        this.geoJsonReader = geoJsonReader;
        this.geojsonStoreConfiguration = geojsonStoreConfiguration.getCollection()
                .stream()
                .collect(Collectors.toMap(GeojsonCollectionSource::getCollectionId, Functions.identity()));
    }

    @SneakyThrows
    @Override
    public List<Feature> fetch(String collectionId,
                               Set<String> ids) {
        if (!geojsonStoreConfiguration.containsKey(collectionId)) {
            throw new WebApplicationException(String.format("Collection %s not configured as a source", collectionId),
                    400);
        }
        val source = geojsonStoreConfiguration.get(collectionId);
        return geoJsonReader.fetchFeatures(source.getUri(), ids);
    }
}
