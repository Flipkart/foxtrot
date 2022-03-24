package com.flipkart.foxtrot.pipeline.geo;

import org.geojson.Feature;

import java.io.IOException;
import java.util.List;

public interface GeoRegionIndex {

    List<Feature> matchingIds(GeoPoint point);

    void index(List<Feature> geoJsonFeatures) throws IOException;
}
