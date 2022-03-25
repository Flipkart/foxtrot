package com.flipkart.foxtrot.pipeline.validator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.Assert.assertEquals;

public class JsonPathStringValidatorTest {

    @Test
    public void testJsonPathValidity() {
        Validator validator = Validation.buildDefaultValidatorFactory()
                .getValidator();
        assertEquals(0, validator.validate(new JsonPathPojo("$.location"))
                .size());
        assertEquals(1, validator.validate(new JsonPathPojo("$location"))
                .size());
        assertEquals(0, validator.validate(new JsonPathPojo("$.location.[0].name"))
                .size());

        assertEquals(0, validator.validate(new JsonPathPojoDefinite("$.location"))
                .size());
        assertEquals(1, validator.validate(new JsonPathPojoDefinite("$location"))
                .size());
        assertEquals(1, validator.validate(new JsonPathPojoDefinite("$.location.[*].name"))
                .size());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JsonPathPojo {

        @ValidJsonPath
        private String path;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JsonPathPojoDefinite {

        @ValidJsonPath(definite = true)
        private String path;
    }
}