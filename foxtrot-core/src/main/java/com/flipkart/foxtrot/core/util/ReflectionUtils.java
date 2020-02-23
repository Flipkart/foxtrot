package com.flipkart.foxtrot.core.util;

import static com.collections.CollectionUtils.nullAndEmptySafeValueList;

import java.lang.reflect.Field;

public class ReflectionUtils {

    private ReflectionUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static <T> Object getField(String fieldName, Class<T> clazz, Object instance) throws IllegalAccessException {
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : nullAndEmptySafeValueList(declaredFields)) {
            if (field.getName().equals(fieldName)) {
                field.setAccessible(true);
                return field.get(instance);
            }
        }
        throw new RuntimeException(String.format("Field %s not found in instance : %s of class : %s",
                fieldName, instance, clazz));
    }

}
