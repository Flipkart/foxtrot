package com.foxtrot.flipkart.translator.config;

import java.util.List;
import lombok.Data;

/***
 Created by nitish.goyal on 28/08/19
 ***/
@Data
public class TranslatorConfig {

    private String rawKeyVersion = "2.0";

    /**
     * List of jsonPaths for fields which need to be unmarshalled to jsonNode
     * e.g. "/eventData/funnelInfo", "/eventData/funnelInfo/funnelData"
     */
    private List<String> unmarshallJsonPaths;

}
