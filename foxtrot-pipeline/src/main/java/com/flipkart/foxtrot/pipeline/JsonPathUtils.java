package com.flipkart.foxtrot.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.experimental.UtilityClass;

import java.util.LinkedHashMap;
import java.util.Optional;

@UtilityClass
public class JsonPathUtils {

    public static void create(DocumentContext context,
                              String path,
                              Object value) {
        int pos = path.lastIndexOf('.');
        String parent = path.substring(0, pos);
        String child = path.substring(pos + 1);
        try {
            context.read(parent);
        } catch (PathNotFoundException e) {
            create(context, parent, new LinkedHashMap<>());
        }
        context.put(parent, child, value);
    }

    public static Optional<JsonNode> readPath(DocumentContext context, String path) {
        try {
            return Optional.ofNullable(context.read(path));
        } catch (PathNotFoundException e) {
            return Optional.empty();
        }
    }
}
