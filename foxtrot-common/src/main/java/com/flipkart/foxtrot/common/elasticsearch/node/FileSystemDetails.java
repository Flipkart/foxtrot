package com.flipkart.foxtrot.common.elasticsearch.node;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileSystemDetails {

    private FileSystemOverview total;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FileSystemOverview {

        @JsonProperty("total_in_bytes")
        private long totalInBytes;

        @JsonProperty("free_in_bytes")
        private long freeInBytes;

        @JsonProperty("available_in_bytes")
        private long availableInBytes;

    }
}
