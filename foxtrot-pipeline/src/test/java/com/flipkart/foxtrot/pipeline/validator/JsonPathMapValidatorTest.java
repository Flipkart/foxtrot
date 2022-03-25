package com.flipkart.foxtrot.pipeline.validator;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonPathMapValidatorTest {

    @Test
    public void testJsonPathValidity() {
        Validator validator = Validation.buildDefaultValidatorFactory()
                .getValidator();
        assertEquals(0,
                validator.validate(new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$.location", "")))
                        .size());
        assertEquals(1,
                validator.validate(new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$location", "")))
                        .size());
        assertEquals(0, validator.validate(
                new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$.location.[0].name", "")))
                .size());

        assertEquals(0,
                validator.validate(new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$.location", "")))
                        .size());
        assertEquals(1,
                validator.validate(new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$location", "")))
                        .size());
        assertEquals(1, validator.validate(
                new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$.location.[*].name", "")))
                .size());

        assertEquals(1, validator.validate(new JsonPathMapValidatorTest.JsonPathPojoDefinite(ImmutableMap.of("$.location.[*].name", "k1", "$location", "k2")))
                .size());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JsonPathPojoDefinite {

        @ValidJsonPath(definite = true)
        private Map<String, String> path;
    }
}