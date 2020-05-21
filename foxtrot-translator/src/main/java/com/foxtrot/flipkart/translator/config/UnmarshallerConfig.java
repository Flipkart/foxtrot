package com.foxtrot.flipkart.translator.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UnmarshallerConfig {

    /**
     * List of jsonPaths for fields in tables which need to be unmarshalled to jsonNode e.g.
     * phonepe_consumer_app_android -> ["/eventData/funnelInfo", "/eventData/funnelInfo/funnelData"]
     */
    Map<String, List<String>> tableVsUnmarshallJsonPath = new HashMap<>();
    private boolean unmarshallingEnabled;
}
