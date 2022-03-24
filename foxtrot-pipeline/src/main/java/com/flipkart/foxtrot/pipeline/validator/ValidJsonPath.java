package com.flipkart.foxtrot.pipeline.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;

@Constraint(validatedBy = {JsonPathStringValidator.class, JsonPathMapValidator.class})
@Target({FIELD, PARAMETER})
@Retention(value = RetentionPolicy.RUNTIME)
public @interface ValidJsonPath {

    String message() default "Some message";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    boolean definite() default false;
}