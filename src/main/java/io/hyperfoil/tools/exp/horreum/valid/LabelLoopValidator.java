package io.hyperfoil.tools.exp.horreum.valid;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import jakarta.enterprise.context.Dependent;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

@Dependent
public class LabelLoopValidator  implements ConstraintValidator<ValidLabel, Label> {
    @Override
    public void initialize(ValidLabel constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(Label label, ConstraintValidatorContext constraintValidatorContext) {
        boolean rtrn = label == null || !label.isCircular();
        return rtrn;
    }
}
