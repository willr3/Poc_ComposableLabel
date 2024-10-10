package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.transaction.Transactional;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class LabelGroupTest {

    @Transactional
    @org.junit.jupiter.api.Test
    public void hasTempLabel_valueExtractor(){
        LabelGroup group = new LabelGroup("group");
        Label first = new Label("first",group).loadExtractors(
                Extractor.fromString("$.foo").setName("foo")
        );
        Label second = new Label("second",group).loadExtractors(
                Extractor.fromString("first:$.bar").setName("bar")
        );
        assertFalse(group.hasTempLabel(first.extractors.get(0)),"jsonpath extractor should not be temp");
        assertTrue(group.hasTempLabel(second.extractors.get(0)),"new value extractor should be temp");



    }
}
