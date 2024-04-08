package io.hyperfoil.tools.exp.horreum.valid;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

@ApplicationScoped
public class LveTargetValidator implements ConstraintValidator<ValidTarget, Extractor> {
    @Override
    public void initialize(ValidTarget constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    //This is validating when added to the Label's extractors list, we need to validate before sending to persistence

    @Override
    public boolean isValid(Extractor extractor, ConstraintValidatorContext constraintValidatorContext) {
        if(extractor instanceof LabelValueExtractor labelValueExtractor) {
            boolean sameTest = labelValueExtractor.parent.parent.equals(labelValueExtractor.targetLabel.parent);
            return sameTest;
        }
        return true;
    }
}
