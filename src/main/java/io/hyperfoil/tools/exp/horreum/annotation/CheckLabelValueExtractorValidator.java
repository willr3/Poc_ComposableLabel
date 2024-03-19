package io.hyperfoil.tools.exp.horreum.annotation;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.*;

@ApplicationScoped
public class CheckLabelValueExtractorValidator implements ConstraintValidator<CheckLabelValueExtractors, Label> {


    @Override
    public void initialize(CheckLabelValueExtractors constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(Label label, ConstraintValidatorContext constraintValidatorContext) {
        boolean isValid = true;
        System.out.println("validating "+label);

        List<Integer> missingLabels = new ArrayList<>();
        for(int i=0; i<label.extractors.size(); i++){
            if(label.extractors.get(i) instanceof LabelValueExtractor){
                LabelValueExtractor e = (LabelValueExtractor) label.extractors.get(i);
                if ( e.targetLabel == null || e.targetLabel.id < 0 || !e.targetLabel.isPersistent()){
                    isValid = false;
                    missingLabels.add(i);
                }
            }
        }


        label.extractors.stream()
                .filter(v -> v instanceof LabelValueExtractor)
                .map(v->(LabelValueExtractor)v)
                .filter(v->v.targetLabel!=null && v.targetLabel.id > -1)
                .forEach(e->{



        });

        if(!isValid){
            constraintValidatorContext.disableDefaultConstraintViolation();
            ConstraintValidatorContext.ConstraintViolationBuilder cvb = constraintValidatorContext.buildConstraintViolationWithTemplate(
                "{io.hyperfoil.tools.exp.horreum.annotation.CheckLabelValueExtractors.message}"
                    );
            cvb.addConstraintViolation();
        }

        return true;
    }
}
