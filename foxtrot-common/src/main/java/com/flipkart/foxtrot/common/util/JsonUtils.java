package com.flipkart.foxtrot.common.util;

import static com.flipkart.foxtrot.common.exception.ErrorCode.DESERIALIZATION_ERROR;
import static com.flipkart.foxtrot.common.exception.ErrorCode.SERIALIZATION_ERROR;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.exception.SerDeException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JsonUtils {

    private JsonUtils() {
        throw new IllegalStateException("Utility class");
    }

    @SneakyThrows
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return SerDe.mapper().readValue(json, clazz);
        } catch (IOException e) {
            log.error("Error while deserializing in fromJson for json: {}, error: {}", json, e);
            throw new SerDeException(DESERIALIZATION_ERROR, "Unable to deserialize data from json");
        }
    }

    @SneakyThrows
    public static <T> T fromJson(byte[] bytes, Class<T> clazz) {
        try {
            return SerDe.mapper().readValue(bytes, clazz);
        } catch (IOException e) {
            log.error("Error while deserializing in fromJson for bytes : {}, error: {}",
                    new String(bytes), e);
            throw new SerDeException(DESERIALIZATION_ERROR, "Unable to deserialize data with class");
        }
    }

    @SneakyThrows
    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return SerDe.mapper().readValue(json, typeReference);
        } catch (IOException e) {
            log.error("Error while deserializing in fromJson for json : {}, error: {}", json, e);
            throw new SerDeException(DESERIALIZATION_ERROR, "Unable to deserialize data with type reference");
        }
    }

    @SneakyThrows
    public static Map<String, Object> readMapFromString(String json) {
        return fromJson(json, new TypeReference<Map<String, Object>>() {
        });
    }

    @SneakyThrows
    public static List<Object> readListFromString(String json) {
        return fromJson(json, new TypeReference<List<Object>>() {
        });
    }

    @SneakyThrows
    public static Map<String, Object> readMapFromObject(Object obj) {
        return toObject(obj, new TypeReference<Map<String, Object>>() {
        });
    }

    @SneakyThrows
    public static List<Map<String, Object>> readListFromObject(Object obj) {
        return toObject(obj, new TypeReference<List<Map<String, Object>>>() {
        });
    }

    @SneakyThrows
    public static <T> T fromJson(byte[] bytes, TypeReference<T> typeReference) {
        try {
            return SerDe.mapper().readValue(bytes, typeReference);
        } catch (IOException e) {
            log.error("Error while deserializing in fromJson for json : {}, error: {}", new String(bytes),
                    e);
            throw new SerDeException(DESERIALIZATION_ERROR, "Unable to deserialize data with type reference");
        }
    }

    @SneakyThrows
    public static String toJson(Object obj) {
        try {
            return SerDe.mapper().writeValueAsString(obj);
        } catch (IOException e) {
            log.error("Error while serializing in toJson for object: {}, error: {}", obj, e);
            throw new SerDeException(SERIALIZATION_ERROR, "Unable to serialize data to json");
        }
    }

    @SneakyThrows
    public static byte[] toBytes(Object obj) {
        try {
            return SerDe.mapper().writeValueAsBytes(obj);
        } catch (IOException e) {
            log.error("Error while serializing in toJson for object : {}, error: {}", obj, e);
            throw new SerDeException(SERIALIZATION_ERROR, "Unable to serialize data from object");
        }
    }

    @SneakyThrows
    public static JsonNode toJsonNode(String json) {
        try {
            return SerDe.mapper().readTree(json);
        } catch (IOException e) {
            log.error("Error while deserializing toJsonNode for json: {}, error: {}", json, e);
            throw new SerDeException(DESERIALIZATION_ERROR, "Unable to deserialize data from json");
        }
    }

    @SneakyThrows
    public static <T> T toObject(Object obj, Class<T> clazz) throws SerDeException {
        try {
            return SerDe.mapper().convertValue(obj, clazz);
        } catch (Exception e) {
            log.error("Error while converting toObject for object: {}, error: {}, {}", obj, e.getCause(),
                    e.getMessage());
            throw new SerDeException(SERIALIZATION_ERROR, "Unable to serialize data with class");
        }
    }

    @SneakyThrows
    public static <T> T toObject(Object obj, TypeReference<T> typeReference) {
        try {
            return SerDe.mapper().convertValue(obj, typeReference);
        } catch (Exception e) {
            log.error("Error while converting toObject for object: {}, error: {}, {}", obj, e.getCause(),
                    e.getMessage());
            throw new SerDeException(SERIALIZATION_ERROR, "Unable to serialize data with type reference");
        }
    }
}
