package com.flipkart.foxtrot.common.elasticsearch.shard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShardMovementRequest {

    private List<MoveCommand> commands;


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MoveCommand {

        private MoveOperation move;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MoveOperation {

        private String index;

        private String shard;

        @JsonProperty("from_node")
        private String fromNode;

        @JsonProperty("to_node")
        private String toNode;
    }

}
