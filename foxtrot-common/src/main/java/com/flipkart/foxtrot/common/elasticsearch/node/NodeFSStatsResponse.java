package com.flipkart.foxtrot.common.elasticsearch.node;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeFSStatsResponse {

    private Map<String, NodeFSDetails> nodes;


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NodeFSDetails {

        public static final String MASTER_ROLE = "master";
        public static final String INGEST_ROLE = "ingest";
        public static final String DATA_ROLE = "data";

        private String name;

        private List<String> roles;

        private FileSystemDetails fs;
    }
}
