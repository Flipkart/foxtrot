package com.flipkart.foxtrot.pipeline.validator;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import lombok.val;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class JsonPathStringValidator implements ConstraintValidator<ValidJsonPath, String> {

    private boolean checkDefinite;

    @Override
    public void initialize(ValidJsonPath constraintAnnotation) {
        checkDefinite = constraintAnnotation.definite();
    }

    @Override
    public boolean isValid(String value,
                           ConstraintValidatorContext context) {
        try {
            val jsonPath = JsonPath.compile(value);
            if (checkDefinite) {
                return jsonPath.isDefinite();
            }
        } catch (InvalidPathException e) {
            return false;
        }
        return true;
    }
}
