package io.hyperfoil.tools.exp.horreum.valid;

import io.quarkus.hibernate.validator.ValidatorFactoryCustomizer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.Valid;
import org.hibernate.validator.BaseHibernateValidatorConfiguration;
import org.hibernate.validator.cfg.ConstraintMapping;

/*@ApplicationScoped*/
public class HorreumValidatorFactoryCustomizer implements ValidatorFactoryCustomizer {
    @Override
    public void customize(BaseHibernateValidatorConfiguration<?> configuration) {
        ConstraintMapping constraintMapping = configuration.createConstraintMapping();

//        constraintMapping
//                .constraintDefinition(Valid.class)
//                .includeExistingValidators(true)
//                .validatedBy(TestLabelValidator.class);
//
//        configuration.addMapping(constraintMapping);


    }
}
