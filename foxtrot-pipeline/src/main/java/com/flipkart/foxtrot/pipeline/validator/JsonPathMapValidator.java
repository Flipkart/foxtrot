package com.flipkart.foxtrot.pipeline.validator;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import lombok.val;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;
import java.util.Map.Entry;

public class JsonPathMapValidator implements ConstraintValidator<ValidJsonPath, Map<String, String>> {

    private boolean checkDefinite;

    @Override
    public void initialize(ValidJsonPath constraintAnnotation) {
        checkDefinite = constraintAnnotation.definite();
    }

    @Override
    public boolean isValid(Map<String, String> value,
                           ConstraintValidatorContext context) {
        try {
            for (Entry<String, String> stringStringEntry : value.entrySet()) {
                val jsonPath = JsonPath.compile(stringStringEntry.getKey());
                if (checkDefinite && !jsonPath.isDefinite()) {
                    return false;
                }
            }

        } catch (InvalidPathException e) {
            return false;
        }
        return true;
    }
}
