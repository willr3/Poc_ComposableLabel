package io.hyperfoil.tools.exp.horreum.annotation;

import jakarta.validation.Payload;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Retention(value = RetentionPolicy.RUNTIME)
@Target({TYPE})
public @interface CheckLabelValueExtractors {

    String message() default "label contains an invalid label value extractor";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
