package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.exp.horreum.entity.*;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LabelServiceTest {
    private static final Logger log = LoggerFactory.getLogger(LabelServiceTest.class);
    @Inject
    EntityManager em;
    @Inject
    TransactionManager tm;
    @Inject
    LabelService labelService;

    @Inject
    Validator validator;


    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_JsonPathExtractor() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        t.loadLabels(a1);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,a1.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals(a1Node,lv.data);
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_no_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");

        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label found = new Label("found",t)
                .loadExtractors(Extractor.fromString("a1").setName("found"));
        t.loadLabels(a1,found);

        Set<ConstraintViolation<Test>> violations = validator.validate(t);

        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();


        assertNotNull(lv,"label_value should exit");
        assertEquals(a1Node,lv.data);
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");

        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label found = new Label("found",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("found"));
        t.loadLabels(a1,found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals("a1_alpha",lv.data.asText());
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_forEach_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");

        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label found = new Label("found",t)
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("found"));
        t.loadLabels(a1,found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();
        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).list();
        assertNotNull(lvs,"label_value should exit");
        assertEquals(3,lvs.size(),lvs.toString());
    }

    //case when m.dtype = 'JsonpathExtractor' and m.jsonpath is not null
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_JsonpathExtractor_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        t.loadLabels(a1);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": \"found\", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(a1,r.id);

        assertTrue(extractedValues.hasNonNull(a1.name),extractedValues.toString());
        assertFalse(extractedValues.getByName(a1.name).get(0).isIterated());
        assertInstanceOf(List.class,extractedValues.getByName(a1.name));
        List<LabelService.ExtractedValue> values = extractedValues.getByName(a1.name);
        assertEquals(1,values.size());
        assertEquals("found",values.get(0).data().asText());
    }
    //case when m.dtype = 'RunMetadataExtractor' and m.jsonpath is not null and m.column_name = 'metadata'
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_RunMetadataExtractor_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label jenkinsBuild = new Label("build",t)
                .loadExtractors(Extractor.fromString(
                        Extractor.METADATA_PREFIX+"metadata"+ Extractor.METADATA_SUFFIX+
                                Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".jenkins.build").setName("build")
                );
        t.loadLabels(jenkinsBuild);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": \"found\", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(jenkinsBuild,r.id);
        assertTrue(extractedValues.hasNonNull(jenkinsBuild.name));
        assertFalse(extractedValues.getByName(jenkinsBuild.name).get(0).isIterated());
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.getByName(jenkinsBuild.name).get(0).data());
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.foreach and jsonb_typeof(m.lv_data) = 'array'


    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.lv_iterated
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_iterated_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label iterA = new Label("iterA",t)
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label foundA = new Label("foundA",t)
                .loadExtractors(Extractor.fromString("iterA:$.key").setName("foundA"));
        t.loadLabels(foundA,iterA,a1);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(foundA,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(foundA.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.getByName(foundA.name).get(0).isIterated());
        assertEquals(3,extractedValues.getByName(foundA.name).size(),"unexpected number of entries in "+extractedValues.getByName(foundA.name));
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != ''
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label firstAKey = new Label("firstAKey",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("firstAKey"));
        t.loadLabels(firstAKey,a1);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);

        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(firstAKey,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(firstAKey.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.getByName(firstAKey.name).get(0).isIterated());
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.getByName(firstAKey.name).get(0).data(),"unexpected: "+extractedValues.getByName(firstAKey.name));
        assertEquals("a1_alpha",((TextNode)extractedValues.getByName(firstAKey.name).get(0).data()).asText());
    }
    //case when m.dtype = 'LabelValueExtractor' and (m.jsonpath is null or m.jsonpath = '')
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_no_jsonpath() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label iterA = new Label("iterA",t)
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        t.loadLabels(iterA,a1);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.getByName(l.name).get(0).isIterated());
        assertEquals(1,extractedValues.getByName(l.name).size(),"unexpected number of entries in "+extractedValues.getByName(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.getByName(l.name).get(0).data(),"unexpected: "+extractedValues.getByName(l.name));
        assertEquals(3,extractedValues.getByName(l.name).get(0).data().size(),"unexpected number of entries in "+extractedValues.getByName(l.name)+"[0]");
    }

    @org.junit.jupiter.api.Test
    public void createLabelValues_doubleIter() throws JsonProcessingException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        //start.txn
        tm.begin();
        Test t = new Test("doubleIter-test");
        Label foo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("foo"));
        Label iterFoo = new Label("iterFoo",t)
                .loadExtractors(Extractor.fromString("foo[]").setName("iterFoo"));
        Label bar = new Label("bar",t)
                .loadExtractors(Extractor.fromString("iterFoo:$.bar").setName("bar"));
        Label iterBar = new Label("iterBar",t)
                .loadExtractors(Extractor.fromString("bar[]").setName("iterBar"));
        Label iterBarSum = new Label("iterBarSum",t)
                .loadExtractors(
                        Extractor.fromString("iterBar:$.key").setName("key"),
                        Extractor.fromString("iterBar:$.value").setName("value")
                );
        iterBarSum.setReducer("({key,value})=>(key||'')+(value||'')");
        t.loadLabels(foo,iterFoo,bar,iterBar,iterBarSum);
        t.persistAndFlush();
        Run r = new Run(
            t.id,
            new ObjectMapper().readTree("""
                {
                  "foo" : [
                    { "bar": [ {"key":"primero"},{"key":"segundo","value":"two"}] }
                  ]
                }
                """),
            new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        //end.txn
        //do stuff outside
        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,iterBarSum.id).list();

        assertEquals(2,lvs.size());
        assertEquals("primero",lvs.get(0).data.asText());
        assertEquals("segundotwo",lvs.get(1).data.asText());
    }

    @org.junit.jupiter.api.Test
    public void labelValues_schema_post_iterated() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Test t = new Test("target_schema");
        Label foo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("foo"));
        Label iterFoo = new Label("iterFoo",t)
                .loadExtractors(Extractor.fromString("foo[]").setName("iterFoo"))
                .setTargetSchema("direct");
        Label biz = new Label("biz",t)
                .loadExtractors(Extractor.fromString("iterFoo:$.biz").setName("biz"));
        Label buz = new Label("buz",t)
                .loadExtractors(Extractor.fromString("iterFoo:$.buz").setName("buz"));
        t.loadLabels(foo,iterFoo,biz,buz);
        t.persistAndFlush();
        Run r = new Run(
            t.id,
            new ObjectMapper().readTree("""
            {
                "foo": [{"biz":"one","buz":"uno"},{"biz":"two","buz":"dos"}]
            }"""),
            new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelService.ValueMap> valueMaps = labelService.labelValues("direct",t.id,Collections.emptyList(),Collections.emptyList());
        assertEquals(2,valueMaps.size());
        assertEquals(new ObjectMapper().readTree(
            """
            {"biz":"one","buz":"uno"}
            """),valueMaps.get(0).data());
        assertEquals(new ObjectMapper().readTree(
            """
            {"biz":"two","buz":"dos"}
            """),valueMaps.get(1).data());
    }

    @org.junit.jupiter.api.Test
    public void labelValues_schema_direct_two_labels() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Test t = new Test("target_schema");
        Label foo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("foo"))
                .setTargetSchema("direct");
        Label bar = new Label("bar",t)
                .loadExtractors(Extractor.fromString("$.bar").setName("bar"))
                .setTargetSchema("direct");
        Label fooBiz = new Label("fooBiz",t)
                .loadExtractors(
                        Extractor.fromString("foo:$.biz").setName("biz"),
                        Extractor.fromString("foo:$.buz").setName("buz")
                );
        Label barBiz = new Label("barBiz",t)
                .loadExtractors(
                        Extractor.fromString("bar:$.biz").setName("biz"),
                        Extractor.fromString("bar:$.buz").setName("buz")
                );
        t.loadLabels(foo,fooBiz,bar,barBiz);
        t.persistAndFlush();
        Run r = new Run(
                t.id,
                new ObjectMapper().readTree("""
            {
                "foo": {"biz":"one","buz":"uno"},
                "bar": {"biz":"two","buz":"dos"}
            }"""),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelService.ValueMap> valueMaps = labelService.labelValues("direct",t.id,Collections.emptyList(),Collections.emptyList());
        assertEquals(2,valueMaps.size());
        assertEquals(new ObjectMapper().readTree(
                """
                {"fooBiz": {"biz":"one","buz":"uno"} }
                """),valueMaps.get(0).data());
        assertEquals(new ObjectMapper().readTree(
                """
                {"barBiz": {"biz":"two","buz":"dos"} }
                """),valueMaps.get(1).data());
    }


    @Transactional
    public Test createTest_reducing(){
        Test t = new Test("reducer-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        a1.reducer= new LabelReducer("ary=>ary.map(v=>v*2)");
        Label iterA = new Label("iterA",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label nxn = new Label("nxn",t)
                .loadExtractors(
                        Extractor.fromString("iterA").setName("foundA"),
                        Extractor.fromString("iterB").setName("foundB")
                );
        nxn.reducer = new LabelReducer("({foundA,foundB})=>foundA*foundB");
        nxn.multiType= Label.MultiIterationType.NxN;
        t.loadLabels(a1,b1,iterA,iterB,nxn); // order should not matter
        t.persist();
        return t;
    }

    @Transactional
    public Test createTest(){
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
                .setTargetSchema("uri:different:keyed")
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
                Extractor.METADATA_PREFIX+"metadata"+ Extractor.METADATA_SUFFIX+
                Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".jenkins.build").setName("build")
            );
        nxn.multiType= Label.MultiIterationType.NxN;

        t.loadLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
        t.persist();
        return t;
    }
    @Transactional
    public Run createRun_reducing(Test t) throws JsonProcessingException {
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"a1\":[0, 2, 4],\"b1\":[1, 3, 5]}"),new ObjectMapper().readTree("{}"));
        r.persist();
        return r;
    }


    @Transactional
    public Run createRun(Test t) throws JsonProcessingException {
        return createRun(t,"");
    }
    @Transactional
    public Run createRun(Test t,String k) throws JsonProcessingException {
        String v = k.isBlank() ? "" : k+"_";
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_"+v+"alpha\"}, {\"key\":\"a1_"+v+"bravo\"}, {\"key\":\"a1_"+v+"charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"b1\": [{\"key\":\"b1_"+v+"alpha\"}, {\"key\":\"b1_"+v+"bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        r.persist();
        return r;
    }
    @Disabled
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_NxN_jsonpath_on_iterated() throws JsonProcessingException {

        Test t = createTest();
        Run r = createRun(t,"uno");
        Label nxn = Label.find("from Label l where l.name=?1 and l.parent.id=?2","nxn",t.id).firstResult();

        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,nxn.id).list();
        assertNotNull(lvs,"label_value should exit");
        //TODO this branch does not yet support NxN
        assertEquals(6,lvs.size(),"expect 6 entries: "+lvs);
//        JsonNode expected = (new ObjectMapper()).readTree("[{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_bravo\"}]");
//        assertEquals(expected,lv.data,"data did not match expected "+lv.data);
    }

    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath_on_iterated() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        Label found = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).list();
        assertNotNull(lvs,"label_value should exit");
        assertEquals(3,lvs.size(),lvs.toString());
    }
    @Disabled
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_NxN_reducing() throws JsonProcessingException {
        Test t = new Test("reducer-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        a1.reducer= new LabelReducer("ary=>ary.map(v=>v*2)");
        Label iterA = new Label("iterA",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label nxn = new Label("nxn",t)
                .loadExtractors(
                        Extractor.fromString("iterA").setName("foundA"),
                        Extractor.fromString("iterB").setName("foundB")
                );
        nxn.reducer = new LabelReducer("({foundA,foundB})=>foundA*foundB");
        nxn.multiType= Label.MultiIterationType.NxN;
        t.loadLabels(a1,b1,iterA,iterB,nxn); // order should not matter
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"a1\":[0, 2, 4],\"b1\":[1, 3, 5]}"),new ObjectMapper().readTree("{}"));
        Run.persist(r);
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelValue> found = LabelValue.find("from LabelValue lv where lv.run.id=?1",r.id).list();
        assertEquals(5,found.size());
        LabelValue lv = LabelValue.find("from LabelValue lv where lv.label.name=?1 and lv.run.id=?2","a1",r.id).singleResult();
        assertNotNull(lv);
        //it applied the 2x function
        assertEquals(new ObjectMapper().readTree("[0,4,8]"),lv.data);
        lv = LabelValue.find("from LabelValue lv where lv.label.name=?1 and lv.run.id=?2","nxn",r.id).singleResult();
        assertNotNull(lv);
        assertEquals(new ObjectMapper().readTree("[0,4,8,0,12,24,0,20,40]"),lv.data);
    }


    @Disabled //this is not a good unit test :)
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_degradation_test() throws JsonProcessingException {
        Test t = createTest();
        Test t2 = createTest();
        Run r2 = createRun(t2,"1");
        labelService.calculateLabelValues(t2.labels,r2.id);
        List<LabelService.ValueMap> t2valueMaps = labelService.labelValues("uri:keyed",t2.id,Collections.emptyList(),Collections.emptyList());
        t2valueMaps.forEach(System.out::println);
        int LIMIT = 5000;
        long length = Math.round(Math.ceil(Math.log10(LIMIT)));
        for(int i=0; i< LIMIT; i++){
            Run r = createRun(t,String.format("%"+length+"d",i));
            long start = System.currentTimeMillis();
            labelService.calculateLabelValues(t.labels,r.id);
            long stop = System.currentTimeMillis();
            double calculateDuration = ((double)stop-start)/1000;
            start = System.currentTimeMillis();
            List<LabelService.ValueMap> valueMaps = labelService.labelValues("uri:keyed",t2.id,Collections.emptyList(),Collections.emptyList());
            stop = System.currentTimeMillis();
            double labelValueDuration = ((double)stop-start)/1000;
            System.out.printf("%"+length+"d %3.3f %3.3f%n",i,calculateDuration,labelValueDuration);
            assertTrue(valueMaps.size()>1,"size = "+valueMaps.size());
            assertTrue((stop-start)/1000 < 1);
        }
    }

    @Transactional
    Test createConflictingExtractorTest(){
        Test t = new Test("example-test");
        Label key = new Label("key",t)
            .loadExtractors(
                Extractor.fromString("$.key1").setName("key"),
                Extractor.fromString("$.key2").setName("key"),
                Extractor.fromString("$.key3").setName("key")
            );
        t.loadLabels(key);
        t.persist();
        return t;
    };
    @Transactional
    Run createConflictingExtractorRun(Test t,String data) throws JsonProcessingException {
        JsonNode node = new ObjectMapper().readTree(data);
        Run r = new Run(t.id,node, JsonNodeFactory.instance.objectNode());
        r.persist();
        return r;
    }
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_extractor_name_conflict_nulls_ignored() throws JsonProcessingException {
        Test t = createConflictingExtractorTest();
        Run r1 = createConflictingExtractorRun(t,"{\"key2\":\"two\"}");
        labelService.calculateLabelValues(t.labels,r1.id);
        Label key = Label.find("from Label l where l.name=?1 and l.parent.id=?2","key",t.id).singleResult();
        List<LabelValue> found = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",key.id,r1.id).list();
        assertNotNull(found);
        assertEquals(1,found.size());
        LabelValue lv = found.get(0);
        assertNotNull(lv.data);
        assertTrue(lv.data.has("key"));
        JsonNode value = lv.data.get("key");
        assertNotNull(value);
        assertInstanceOf(TextNode.class,value);
        assertEquals("two",value.asText());
    }
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_extractor_name_conflict_last_match_wins() throws JsonProcessingException {
        Test t = createConflictingExtractorTest();
        Run r1 = createConflictingExtractorRun(t,"{\"key1\":\"one\",\"key2\":\"two\",\"key3\":\"three\"}");
        labelService.calculateLabelValues(t.labels,r1.id);
        Label key = Label.find("from Label l where l.name=?1 and l.parent.id=?2","key",t.id).singleResult();
        List<LabelValue> found = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",key.id,r1.id).list();
        assertNotNull(found);
        assertEquals(1,found.size());
        LabelValue lv = found.get(0);
        assertNotNull(lv.data);
        assertTrue(lv.data.has("key"));
        JsonNode value = lv.data.get("key");
        assertNotNull(value);
        assertInstanceOf(TextNode.class,value);
        assertEquals("three",value.asText());
    }


    //TODO this count is expecting NvN calculations to be correct
    @Disabled
    @org.junit.jupiter.api.Test
    public void getDerivedValues_iterA() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        labelService.calculateLabelValues(t.labels,r2.id);
        Label iterA = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        Label nxn = Label.find("from Label l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();
        Label foundA = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        List<LabelValue> labelValues = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",iterA.id,r1.id).list();
        List<LabelValue> found = labelService.getDerivedValues(labelValues.get(0),0);
        found.forEach(System.out::println);
        assertFalse(found.isEmpty(),"found should not be empty");
        assertEquals(7,found.size());
        assertTrue(found.stream().anyMatch(lv->lv.label.equals(nxn)));
        assertTrue(found.stream().anyMatch(lv->lv.label.equals(foundA)));
    }
    @org.junit.jupiter.api.Test
    public void getBySchema_multiple_results() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelValue> found = labelService.getBySchema("uri:keyed",t.id);

        assertFalse(found.isEmpty(),"found should not be empty");
        //TODO 5 is becasue we do not yet process NxN
        assertEquals(5,found.size(),"found should have 2 entries: "+found);
    }

    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_RunMetadataExtractor() throws JsonProcessingException {
        Test t = new Test("example-test");

        Label found = new Label("found",t)
                .loadExtractors(Extractor.fromString("{metadata}:$.jenkins.build").setName("found"));
        t.loadLabels(found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals("123",lv.data.asText());
    }

    @org.junit.jupiter.api.Test
    public void labelValues_schema() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        List<LabelService.ValueMap> labelValues = labelService.labelValues("uri:keyed",t.id, Collections.emptyList(),Collections.emptyList());
        long aCount = labelValues.stream().filter(map->map.data().has("foundA")).count();
        long bCount = labelValues.stream().filter(map->map.data().has("foundB")).count();

        assertEquals(3,aCount);
        assertEquals(2,bCount);
        assertEquals(5,labelValues.size());
    }
    @org.junit.jupiter.api.Test
    public void labelValues_testId() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        labelService.calculateLabelValues(t.labels,r1.id);
        List<LabelService.ValueMap> labelValues = labelService.labelValues(t.id,null,null,null,null,null,Integer.MAX_VALUE,0,Collections.emptyList(),Collections.emptyList(),false);
        assertEquals(1,labelValues.size(),"only one run should exist for the test");
        LabelService.ValueMap map = labelValues.get(0);
        assertEquals(t.id,map.testId(),"test Id should match");
        assertEquals(r1.id,map.runId(),"run Id should match");
        ObjectNode data = map.data();
        assertNotNull(data,"data should not be null");
        assertTrue(data.has("nxn"),"data should have the nxn key");
        assertTrue(data.has("iterA"),"data should have the iterA key");
        assertTrue(data.has("iterB"),"data should have the iterB key");
        assertTrue(data.has("iterAKey"),"data should have the iterAKey key");
        assertTrue(data.has("foundA"),"data should have hte foundA key");
        assertTrue(data.has("foundB"),"data should have hte foundB key");
    }

    @org.junit.jupiter.api.Test
    public void labelValues_label() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        List<LabelService.ValueMap> labelValues = labelService.labelValues(l.id, r1.id,t.id, Collections.emptyList(),Collections.emptyList());
        labelValues.forEach(System.out::println);
        assertEquals(3,labelValues.size());
    }
    @Disabled
    @org.junit.jupiter.api.Test
    public void multiType_NxN() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();
        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r1.id,l.id).firstResult();

        //expect 6 entries given 3 from iterA and 2 from iterB
        assertEquals(6,lv.data.size());
        //make sure the data contains the proper 3 x 2 combinations
        JsonNode expected = (new ObjectMapper()).readTree("[{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_bravo\"}]");
        assertEquals(expected,lv.data,"data did not match expected "+lv.data);
    }

    @org.junit.jupiter.api.Test
    public void getDescendantLabels() throws JsonProcessingException {
        Test t = createTest();

        Label a1 = Label.find("from Label l where l.name=?1 and l.parent.id=?2","a1",t.id).singleResult();
        Label firstAKey = Label.find("from Label l where l.name=?1 and l.parent.id=?2","firstAKey",t.id).singleResult();
        Label justA = Label.find("from Label l where l.name=?1 and l.parent.id=?2","justA",t.id).singleResult();
        Label iterA = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        Label iterAKey = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterAKey",t.id).singleResult();
        Label foundA = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        Label nxn = Label.find("from Label l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();

        List<Label> list = labelService.getDescendantLabels(a1.id);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        List<Label> expected = Arrays.asList(a1,firstAKey,justA,iterA,iterAKey,foundA,nxn);
        assertEquals(expected.size(),list.size(),list.size() > expected.size() ? "extra "+list.stream().filter(v->!expected.contains(v)).toList() : "missing "+expected.stream().filter(v->!list.contains(v)).toList());
        expected.forEach(l->{
            assertTrue(list.contains(l),"descendant should include "+l.name+" : "+list);
        });

    }



}
