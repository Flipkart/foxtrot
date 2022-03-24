package com.flipkart.foxtrot.pipeline.resources;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@Builder
public class GeojsonStoreConfiguration {

    private List<GeojsonCollectionSource> collection;

    public GeojsonStoreConfiguration() {
        collection = new ArrayList<>();
    }
}
