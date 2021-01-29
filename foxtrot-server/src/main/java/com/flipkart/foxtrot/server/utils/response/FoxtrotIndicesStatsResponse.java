package com.flipkart.foxtrot.server.utils.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FoxtrotIndicesStatsResponse {

    private IndicesStatsResponse indicesStatsResponse;
    private Map<String, Integer> tableColumnCount;

}
