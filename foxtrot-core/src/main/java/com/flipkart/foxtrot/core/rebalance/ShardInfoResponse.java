package com.flipkart.foxtrot.core.rebalance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardInfoResponse {

    private String index;
    private String shard;
    private String node;
    private String state;

    @JsonProperty("prirep")
    private String primaryOrReplica;

    public enum ShardType {
        PRIMARY("p"),
        REPLICA("r");


        @Getter
        private String name;

        ShardType(String name) {
            this.name = name;
        }
    }
}
