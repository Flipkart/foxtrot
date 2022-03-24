package com.flipkart.foxtrot.pipeline.resources;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GeojsonFetchRequest {

    private Set<String> ids;
}
