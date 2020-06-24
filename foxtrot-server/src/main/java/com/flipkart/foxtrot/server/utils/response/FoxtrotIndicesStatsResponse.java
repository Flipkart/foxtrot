package com.flipkart.foxtrot.server.utils.response;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FoxtrotIndicesStatsResponse {

    private IndicesStatsResponse indicesStatsResponse;
    private Map<String, Integer> tableColumnCount;

}
