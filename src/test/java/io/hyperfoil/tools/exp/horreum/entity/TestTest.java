package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class TestTest {


    @Transactional
    @org.junit.jupiter.api.Test
    public void label_references_peer_label(){
        Test t = new Test("example-test");
        t.setLabels(
                //added before the label it references but that should not cause a problem
                new Label("bar",t).loadExtractors(Extractor.fromString("foo:$.bar")),
                new Label("foo",t).loadExtractors(Extractor.fromString("$.foo"))

        );
        try{
            t.persistAndFlush();
        }catch (Exception e){
            fail(e.getMessage(),e);
        }

        long created = Label.find("from Label l where l.name=?1 and l.group.id=?2","foo",t.id).count();
        //we cannot check that
        assertEquals(1,created,"should only create one label");

    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void invalid_missing_targetLabel() {
        Test t = new Test("example-test");
                t.setLabels(
                        new Label("missing",t)
                                .loadExtractors(Extractor.fromString("doesNotExist:$.foo"))
                );
        try {
            t.persistAndFlush();
            fail("should throw an exception");
        }catch(ConstraintViolationException ignored){}
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void setLabels_prevent_circular(){
        Test t= new Test("example-test");
        Label l1 = new Label("foo",t);
        Extractor lve1 = new Extractor();
        lve1.type = Extractor.Type.VALUE;
        Label l2 = new Label("bar",t);
        Extractor lve2 = new Extractor();
        lve2.type = Extractor.Type.VALUE;

        lve1.name="lve1";
        lve1.targetLabel=l2;
        l1.loadExtractors(lve1);
        lve2.name="lve2";
        lve2.targetLabel=l1;
        l2.loadExtractors(lve2);

        try {
            t.setLabels(l1, l2);
            assertNotNull(t);
            assertEquals(2,t.size());
            t.persistAndFlush();//
            fail("validation check on looped labels should prevent this");
        }catch(ConstraintViolationException ignored){}
    }

    @org.junit.jupiter.api.Test
    public void setLabels_sorted(){
        Test t = new Test("foo");
        Label l1 = new Label("foo").loadExtractors(Extractor.fromString("$.foo"));
        Label l2 = new Label("buz").loadExtractors(Extractor.fromString("bar:$.bar"));
        Label l3 = new Label("bar").loadExtractors(Extractor.fromString("foo:$.bar"));

        t.setLabels(l1,l2,l3);

        assertTrue(l1 == t.labels.get(0),"foo should be first");
        assertTrue(l3 == t.labels.get(1),"bar should be second");
        assertTrue(l2 == t.labels.get(2),"buz should be last");
    }
    @org.junit.jupiter.api.Test
    public void setLabels_2Nd_generation_dependency(){
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
        t.setLabels(nxn,found,iterB,iterA,b1,a1);
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
}
