package com.foxtrot.flipkart.translator.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 28/08/19
 ***/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TranslatorConfig {

    private String rawKeyVersion = "2.0";

    private UnmarshallerConfig unmarshallerConfig = new UnmarshallerConfig();

}
