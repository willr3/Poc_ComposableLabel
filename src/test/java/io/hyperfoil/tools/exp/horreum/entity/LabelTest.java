package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.RunMetadataExtractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Disabled;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LabelTest {


    public static class CompareLabels implements Comparator<Label> {

        @Override
        public int compare(Label o1, Label o2) {
            int rtrn = 0;
            if(o1.usesLabelValueExtractor() && !o2.usesLabelValueExtractor()){
                rtrn = -1;//o1 is less than 02
            }else if (o2.usesLabelValueExtractor() && !o1.usesLabelValueExtractor()){
                rtrn = 1;//o2 is less than o1
            }else if (o1.dependsOn(o2)){
                rtrn = -1;//o1 has to come after o2
            }else if (o2.dependsOn(o1)) {
                rtrn = 1;//o1 must come before o2
            }
            return rtrn;
        }
    }

    @Inject
    Validator validator;

    @org.junit.jupiter.api.Test
    public void invalid_null_extractor(){
        Label l = new Label();
        List<Extractor> extractorList = new ArrayList<>();
        extractorList.add(null);
        l.extractors = extractorList;
        Set<ConstraintViolation<Label>> violations = validator.validate(l);
        assertFalse(violations.isEmpty(),violations.toString());
    }
    @org.junit.jupiter.api.Test
    public void compareTo_sortOrder(){
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label firstAKey = new Label("firstAKey",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("firstAKey"));
        Label justA = new Label("justA",t)
                .loadExtractors(Extractor.fromString("a1").setName("justA"));
        Label iterA = new Label("iterA",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterAKey = new Label("iterAKey",t)
                //.setTargetSchema("uri:keyed") // causes an error when it targets a schema
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("iterAKey"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label foundA = new Label("foundA",t)
                .loadExtractors(Extractor.fromString("iterA:$.key").setName("foundA"));
        Label foundB = new Label("foundB",t)
                .loadExtractors(Extractor.fromString("iterB:$.key").setName("foundB"));
        Label nxn = new Label("nxn",t)
                .loadExtractors(
                        Extractor.fromString("iterA:$.key").setName("foundA"),
                        Extractor.fromString("iterB:$.key").setName("foundB")
                );
        Label jenkinsBuild = new Label("build",t)
                .loadExtractors(Extractor.fromString(
                        RunMetadataExtractor.PREFIX+"metadata"+RunMetadataExtractor.SUFFIX+
                                LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".jenkins.build").setName("build")
                );
        nxn.multiType= Label.MultiIterationType.NxN;

        t.loadLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter


        int nxnIndex = t.labels.indexOf(nxn);
        int foundAIndex = t.labels.indexOf(foundA);
        int foundBIndex = t.labels.indexOf(foundB);
        int iterAKeyIndex = t.labels.indexOf(iterAKey);
        int a1Index = t.labels.indexOf(a1);
        int b1Index = t.labels.indexOf(b1);
        int iterAIndex = t.labels.indexOf(iterA);
        int iterBindex = t.labels.indexOf(iterB);

        assertTrue(nxnIndex > iterAIndex);
        assertTrue(nxnIndex > iterBindex);
        assertTrue(foundAIndex > iterAIndex);
        assertTrue(foundBIndex > iterBindex);
        assertTrue(a1Index < iterAIndex);
        assertTrue(b1Index < iterBindex);
        assertTrue(iterAKeyIndex > a1Index);

    }

    @org.junit.jupiter.api.Test
    public void compareTo_dependsOn_two(){
        Label a1 = new Label("a1")
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label b1 = new Label("b1")
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label iterA = new Label("iterA")
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterB = new Label("iterB")
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label found = new Label("found")
                .loadExtractors(Extractor.fromString("iterA:$.key").setName("found"));
        Label nxn = new Label("nxn")
                .loadExtractors(
                        Extractor.fromString("iterA:$.key").setName("foundA"),
                        Extractor.fromString("iterB:$.key").setName("foundB")
                );
        Test t = new Test("example-test");
        t.loadLabels(nxn,found,iterB,iterA,b1,a1);
        List<Label> list = new ArrayList<>(t.labels);

        int a1Index = list.indexOf(a1);
        int b1Index = list.indexOf(b1);
        int iterAIndex = list.indexOf(iterA);
        int iterBIndex = list.indexOf(iterB);
        int foundIndex = list.indexOf(found);
        int nxnIndex = list.indexOf(nxn);

        assertTrue(a1Index < iterAIndex);
        assertTrue(b1Index < iterBIndex);
        assertTrue(foundIndex > iterAIndex);
        assertTrue(nxnIndex > iterAIndex);
        assertTrue(nxnIndex > iterBIndex);
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
    public void compareTo_same_name_and_targetLabel_different_jsonpath(){
        Label l1 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.foo"));
        Label l2 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));

        List<Label> list = new ArrayList<>(Arrays.asList(l1,l2));
        list.sort(Label::compareTo);
        //assume stable sort
        assertTrue(l1 == list.get(0),"foo:$.foo should be first");
        assertTrue(l2 == list.get(1),"foo:$.bar should be second");
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
    public void isCircular_unexpected_exception(){
        Test t = new Test("example-test");
        Label l1 = new Label("foo",t).loadExtractors(Extractor.fromString("$.foo"));
        Label l2 = new Label("bar",t).loadExtractors(Extractor.fromString("foo:$.bar"));
        t.loadLabels(l1,l2);

        assertFalse(l1.isCircular());
        assertFalse(l2.isCircular());

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
        ex = Extractor.fromString("foo");
        assertInstanceOf(LabelValueExtractor.class, ex, "foo should be a label value extractor");

    }

}
