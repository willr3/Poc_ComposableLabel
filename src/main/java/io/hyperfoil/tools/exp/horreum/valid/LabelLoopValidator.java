package io.hyperfoil.tools.exp.horreum.valid;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
//@ApplicationScoped
@Dependent //used if it depends on the content of the annotation
public class LabelLoopValidator  implements ConstraintValidator<ValidLabel, Label> {
    @Override
    public void initialize(ValidLabel constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(Label label, ConstraintValidatorContext constraintValidatorContext) {
        boolean rtrn = true;
        if(label==null){
            constraintValidatorContext.buildConstraintViolationWithTemplate("null labels are not valid").addConstraintViolation();
            return false;
        }
        if(label.isCircular()){
            constraintValidatorContext.buildConstraintViolationWithTemplate("labels cannot create a circular reference with other label's extractors").addConstraintViolation();
            rtrn =false;
        }
        if(label.name == null || label.name.isBlank()){
            constraintValidatorContext.buildConstraintViolationWithTemplate("label names cannot be null or empty").addConstraintViolation();
        }
        if(label.name.startsWith("$")){

        }
        return rtrn;
    }
}
