package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.Run;
import io.hyperfoil.tools.exp.horreum.entity.Test;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@QuarkusTest
public class TestServiceTest {
    @Inject
    EntityManager em;

    @Inject
    TestService service;

    @Transactional
    @org.junit.jupiter.api.Test
    public void invalid_missing_targetLabel() {
        Test t = new Test("example-test")
                .loadLabels(
                        new Label("missing")
                                .loadExtractors(Extractor.fromString("doesNotExist:$.foo"))
                );
        Test.persist(t);
    }

    @Transactional
    @org.junit.jupiter.api.Test
    public void load() throws JsonProcessingException {
        Test t = new Test("example-test")
        .loadLabels(
            new Label("foo")
                .loadExtractors(Extractor.fromString("$.foo")),
            new Label("a1")
                .loadExtractors(Extractor.fromString("$.a1")),
            new Label("a2")
                .loadExtractors(Extractor.fromString("$.a2")),
            new Label("bar")
                    .loadExtractors(Extractor.fromString("$.foo.bar"))
        );

        Test.persist(t);

        System.out.println("Persisted with:");
        t.labels.forEach(l->{
            System.out.println(l);
        });

        Label bar = new Label("bar")
            .loadExtractors(Extractor.fromString("foo:$.bar"));

        Label alpha = new Label("alpha")
            .loadExtractors(Extractor.fromString("$.foo").setName("f"),Extractor.fromString("$.fizz").setName("z"));

        Label bravo = new Label("iter_scalar_default")
                .loadExtractors(Extractor.fromString("a1[]:"),Extractor.fromString("$.foo.bar"));
        Label charlie = new Label("iter_iter_default")
                .loadExtractors(Extractor.fromString("a1[]:"),Extractor.fromString("a2"+ LabelValueExtractor.FOR_EACH_SUFFIX));

        List<Label> defs = new ArrayList<>();
        defs.addAll(t.labels);
        defs.addAll(Arrays.asList(bar,alpha,bravo,charlie));

        t.labels = defs;

        Test.persist(t);

        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ \"a1_alpha\", \"a1_bravo\", \"a1_charlie\"], \"a2\": [\"a2_alpha\", \"a2_bravo\"] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        service.calculateLabelValues(t,r.id);


        //calculate labelValues


    }
}
