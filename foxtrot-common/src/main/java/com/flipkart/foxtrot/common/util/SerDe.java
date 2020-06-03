package com.flipkart.foxtrot.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class SerDe {

    private static ObjectMapper mapper;

    private SerDe() {
        throw new IllegalStateException("Utility class");
    }

    public static void init(ObjectMapper objectMapper) {
        mapper = objectMapper;
    }

    public static ObjectMapper mapper() {
        Preconditions.checkNotNull(mapper, "Please call SerDe.init(mapper) to set mapper");
        return mapper;
    }
}
