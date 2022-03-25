package com.flipkart.foxtrot.common.elasticsearch.index;

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
public class IndexInfoResponse {

    private String index;
    private String status;
    private String health;

    @JsonProperty("docs.count")
    private long docCount;

    @JsonProperty("store.size")
    private long storeSize;

    @JsonProperty("pri.store.size")
    private long primaryStoreSize;
}
