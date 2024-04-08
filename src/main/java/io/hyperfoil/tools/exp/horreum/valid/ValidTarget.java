package io.hyperfoil.tools.exp.horreum.valid;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE_USE, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = LveTargetValidator.class)
public @interface ValidTarget {
    String message() default "LabelValueExtractors must extract from labels in the same Test";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
