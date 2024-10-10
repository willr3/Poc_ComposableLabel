package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.Test;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;


import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ExtractorTest {

    @Inject
    EntityManager em;
    @Inject
    TransactionManager tm;


    @Transactional
    public Extractor createExtractor(){

        Test t = new Test("example-test");
        Label l = new Label("foo",t);
        Extractor e = Extractor.fromString("$.foo");
        l.loadExtractors(e);
        t.setLabels(l);
        t.persistAndFlush();

        Extractor rme = new Extractor();
        rme.type = Extractor.Type.METADATA;
        rme.name = e.name;
        rme.parent = e.parent;
        rme.id = e.id;
        rme.column_name="metadata";
        rme.jsonpath="$.bar";

        Extractor postMerge = Extractor.getEntityManager().merge(rme);
        e.name = "foobar";
        e = Extractor.getEntityManager().merge(e);
        return e;
    }

    @org.junit.jupiter.api.Test
    public void copy_path() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Test t = new Test("copy_path");
        Label l = new Label("copy_path_label",t);

        Extractor original = Extractor.fromString("$.foo");
        l.loadExtractors(original);
        t.setLabels(l);
        t.persistAndFlush();

        tm.commit();

        Extractor copy = original.copy(Label.DEFAULT_RESOLVER);

        assertFalse(original.id == copy.id,"copy id should be different than original");

        assertEquals(original.name,copy.name,"name should be the same");
        assertEquals(original.type,copy.type,"type should be the same");
        assertEquals(original.jsonpath,copy.jsonpath,"jsonpath should be the same");
        assertEquals(original.forEach,copy.forEach,"forEach should be the same");
        assertNull(copy.targetLabel,"copy should not have a targetLabel");
        assertNull(copy.column_name,"copy should not have a column_name");
    }

    @org.junit.jupiter.api.Test
    public void copy_value() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Test t = new Test("copy_value");

        Label foo = new Label("foo",t);
        Label bar = new Label("bar",t);
        Extractor original = Extractor.fromString("foo:$.bar");
        bar.loadExtractors(original);
        t.setLabels(foo,bar);
        t.persistAndFlush();
        tm.commit();

        Label fake = new Label("fake");
        Map<String,Label> sources = new HashMap<>();
        sources.put("foo",fake);

        Extractor copy = original.copy(sources::get);
        assertFalse(original.id == copy.id,"copy id should be different than original");
        assertEquals(original.name,copy.name,"name should be the same");
        assertEquals(original.type,copy.type,"type should be the same");
        assertEquals(original.jsonpath,copy.jsonpath,"jsonpath should be the same");
        assertEquals(original.forEach,copy.forEach,"forEach should be the same");
        assertEquals(fake,copy.targetLabel);
        assertNotEquals(foo.id,fake.id,"target Label ids should be different");
    }



    @Transactional
    @org.junit.jupiter.api.Test
    public void onUpdate(){
        Extractor e = createExtractor();
    }

}
