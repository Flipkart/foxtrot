package com.flipkart.foxtrot.common.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;

import java.io.IOException;

/***
 Created by nitish.goyal on 06/07/19
 ***/
public class StatsValueDeserializer extends KeyDeserializer {

    @Override
    public Number deserializeKey(String key,
                                 DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (key == null) {
            return null;
        }
        return Double.parseDouble(key);
    }
}
