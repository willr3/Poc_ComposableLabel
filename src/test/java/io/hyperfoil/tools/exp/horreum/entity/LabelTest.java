package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.RunMetadataExtractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LabelTest {

    @Inject
    Validator validator;

    @org.junit.jupiter.api.Test
    public void invalid_null_extractor(){
        Label l = new Label();
        List<Extractor> extractorList = new ArrayList<>();
        extractorList.add(null);
        l.extractors = extractorList;
        Set<ConstraintViolation<Label>> violations = validator.validate(l);
        System.out.println(violations);
    }

    @org.junit.jupiter.api.Test
    public void compareTo_dependsOn(){
        Label l1 = new Label("foo").loadExtractors(Extractor.fromString("$.foo"));
        Label l2 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));

        int comp = 0;
        comp = l1.compareTo(l2);
        assertTrue(comp < 0,"expect l1 to be before l2 but compareTo was "+comp);
        comp = l2.compareTo(l1);
        assertTrue(comp > 0,"expect l2 to be after l1 but compareTo was "+comp);
    }

    @org.junit.jupiter.api.Test
    public void compareTo_labelValueExtractor(){
        Label l1 = new Label("foo").loadExtractors(Extractor.fromString("$.foo"));
        Label l2 = new Label("bar").loadExtractors(Extractor.fromString("buz:$.bar"));

        int comp = 0;
        comp = l1.compareTo(l2);
        assertTrue(comp < 0,"expect l1 to be before l2 but compareTo was "+comp);
        comp = l2.compareTo(l1);
        assertTrue(comp > 0,"expect l2 to be after l1 but compareTo was "+comp);
    }
    @org.junit.jupiter.api.Test
    public void compareTo_both_labelValueExtractor(){
        Label l1 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));
        Label l2 = new Label("buz").loadExtractors(Extractor.fromString("bar:$.bar"));

        int comp = 0;
        comp = l1.compareTo(l2);
        assertTrue(comp < 0,"expect l1 to be before l2 but compareTo was "+comp);
        comp = l2.compareTo(l1);
        assertTrue(comp > 0,"expect l2 to be after l1 but compareTo was "+comp);
    }
    @org.junit.jupiter.api.Test
    public void dependsOn_both_labelValueExtractor(){
        Label l2 = new Label("buz").loadExtractors(Extractor.fromString("bar:$.bar"));
        Label l3 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));

        assertTrue(l2.dependsOn(l3));
        assertFalse(l3.dependsOn(l2));
    }

    @org.junit.jupiter.api.Test
    public void compareTo_sortedList(){
        Label l1 = new Label("foo").loadExtractors(Extractor.fromString("$.foo"));
        Label l2 = new Label("buz").loadExtractors(Extractor.fromString("bar:$.bar"));
        Label l3 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));

        List<Label> list = new ArrayList<>(Arrays.asList(l1,l2,l3));
        list.sort(Label::compareTo);

        assertTrue(l1 == list.get(0),"foo should be first");
        assertTrue(l3 == list.get(1),"bar should be second");
        assertTrue(l2 == list.get(2),"buz should be last");
    }

    @org.junit.jupiter.api.Test
    public void isCircular_pass(){
        //l1
        Label l1 = new Label("l1").loadExtractors(Extractor.fromString("$.foo"));
        assertFalse(l1.isCircular(),"label without label value extractor");

        //l2 -> l1 (l2 depends on l1)
        LabelValueExtractor lve2_1 = new LabelValueExtractor();
        lve2_1.targetLabel = l1;
        lve2_1.name="lve";
        Label l2 = new Label("l2");
        l2.extractors = Arrays.asList(lve2_1);
        assertFalse(l1.isCircular(),"label without label value extractor");
        assertFalse(l2.isCircular(),"ancestor is not");


        //l3 -> [l2,l1], l2 -> l1 (l3 depends on l2 and l1, l2 depends on l1
        LabelValueExtractor lve3_1 = new LabelValueExtractor();
        lve3_1.targetLabel = l1;
        lve3_1.name="lve3_1";

        LabelValueExtractor lve3_2 = new LabelValueExtractor();
        lve3_2.targetLabel = l2;
        lve3_2.name="lve3_2";

        Label l3 = new Label("l3");
        l3.extractors = Arrays.asList(lve3_1,lve3_2);

        assertFalse(l1.isCircular(),"shared ancestor is not circular");
        assertFalse(l2.isCircular(),"shared ancestor is not circular");
        assertFalse(l3.isCircular(),"shared ancestor is not circular");
    }

    @org.junit.jupiter.api.Test
    public void isCircular_pass_duplicate_dependency(){
        Label l1 = new Label("l1").loadExtractors(Extractor.fromString("$.foo"));;

        //l2 -> [l1, l1]
        Label l2 = new Label("l2");
        LabelValueExtractor lve2_1 = new LabelValueExtractor();
        lve2_1.targetLabel = l1;
        lve2_1.name="lve2_1";
        LabelValueExtractor lve2_2 = new LabelValueExtractor();
        lve2_2.targetLabel = l1;
        lve2_2.name="lve2_1";

        l2.extractors=Arrays.asList(lve2_1,lve2_2);

        assertFalse(l1.isCircular(),"only label value extractors can be circular");
        assertFalse(l2.isCircular(),"having duplicate dependencies is not circular");

    }

    @org.junit.jupiter.api.Test
    public void isCircular_self_reference(){
        Label l = new Label("foo");
        LabelValueExtractor lve = new LabelValueExtractor();
        lve.name="lve";
        lve.targetLabel=l;
        l.extractors = Arrays.asList(lve);

        assertTrue(l.isCircular(),"self referencing is circular");
    }

    @org.junit.jupiter.api.Test
    public void isCircular_circular_pair(){
        Label l1 = new Label("foo");
        LabelValueExtractor lve1 = new LabelValueExtractor();
        Label l2 = new Label("bar");
        LabelValueExtractor lve2 = new LabelValueExtractor();

        lve1.name="lve1";
        lve1.targetLabel=l2;
        l1.extractors=Arrays.asList(lve1);
        lve2.name="lve2";
        lve2.targetLabel=l1;
        l2.extractors=Arrays.asList(lve2);

        assertTrue(l1.isCircular(),"pair reference is circular");
        assertTrue(l2.isCircular(),"pair reference is circular");

    }
    @org.junit.jupiter.api.Test
    public void isCircular_circular_trio(){
        Label l1 = new Label("foo");
        LabelValueExtractor lve1 = new LabelValueExtractor();
        Label l2 = new Label("bar");
        LabelValueExtractor lve2 = new LabelValueExtractor();
        Label l3 = new Label("biz");
        LabelValueExtractor lve3 = new LabelValueExtractor();

        lve1.name="lve1";
        lve1.targetLabel=l2;
        l1.extractors=Arrays.asList(lve1);
        lve2.name="lve2";
        lve2.targetLabel=l3;
        l2.extractors=Arrays.asList(lve2);
        lve3.name="lve3";
        lve3.targetLabel=l1;
        l3.extractors=Arrays.asList(lve3);

        assertTrue(l1.isCircular(),"trio reference is circular");
        assertTrue(l2.isCircular(),"trio reference is circular");
        assertTrue(l3.isCircular(),"trio reference is circular");
    }

    @org.junit.jupiter.api.Test
    public void fromString_instanceof(){
        Extractor ex;
        ex = Extractor.fromString(LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".foo.bar");
        assertInstanceOf(JsonpathExtractor.class, ex, LabelValueExtractor.FOR_EACH_SUFFIX + LabelValueExtractor.NAME_SEPARATOR + JsonpathExtractor.PREFIX + ".foo.bar should be jsonpath extractor");
        ex = Extractor.fromString(RunMetadataExtractor.PREFIX+"metadata"+RunMetadataExtractor.SUFFIX+LabelValueExtractor.NAME_SEPARATOR+JsonpathExtractor.PREFIX+".foo.bar");
        assertInstanceOf(RunMetadataExtractor.class, ex, RunMetadataExtractor.PREFIX + "metadata" + RunMetadataExtractor.SUFFIX + LabelValueExtractor.NAME_SEPARATOR + JsonpathExtractor.PREFIX + ".foo.bar should be run metadata extractor");


    }

}
