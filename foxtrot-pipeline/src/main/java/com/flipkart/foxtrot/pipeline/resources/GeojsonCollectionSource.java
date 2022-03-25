package com.flipkart.foxtrot.pipeline.resources;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GeojsonCollectionSource {
    private String collectionId;
    private String name;
    private String uri;

}
