package com.flipkart.foxtrot.pipeline.resources;

import org.geojson.Feature;

import java.util.List;
import java.util.Set;

public interface GeojsonStore {
    List<Feature> fetch(String collectionId, Set<String> ids);
}
