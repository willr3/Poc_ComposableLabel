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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    @org.junit.jupiter.api.Test
    public void whatCanWeFind_simple() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        LabelGroup group = new LabelGroup("getFqdn_justName");
        LabelGroup source1 = new LabelGroup("aGroup");
        Label first = new Label("label_1",group);
        first.sourceGroup=source1;

        LabelGroup source2 = new LabelGroup("bGroup");
        Label second = new Label("label_2",group);
        second.sourceGroup=source2;


        LabelGroup source3 = new LabelGroup("cGroup");
        Label third = new Label("label_3",group);
        third.sourceGroup=source3;


        Label fourth = new Label("label_4",group);
        fourth.sourceGroup=source3;


        second.sourceLabel=first;
        third.sourceLabel=second;
        fourth.sourceLabel=third;

        fourth.persistAndFlush();
        second.persistAndFlush();
        first.persistAndFlush();
        third.persistAndFlush();
        tm.commit();

        List<Label> found = labelService.whatCanWeFind("label_3",group.id);
        assertEquals(2,found.size(),"expect to find 2 labels");
        assertEquals(third.name,found.get(0).name,"first entry should be label_3");
        assertEquals(fourth.name,found.get(1).name,"second entry shoudl be label_4");
    }

    @Transactional
    @org.junit.jupiter.api.Test
    public void findGroup_multiple_scope(){
        LabelGroup nope = new LabelGroup("bar");
        nope.owner="wrong";
        LabelGroup local = new LabelGroup("bar");
        local.owner="foo";
        LabelGroup pub = new LabelGroup("bar");
        pub.owner="public";
        nope.persist();
        local.persist();
        pub.persist();

        List<LabelGroup> found = labelService.findGroup("bar","foo");
        System.out.println(found.size());
        found.forEach(f-> System.out.println(f.name+" "+f.id));
    }

    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_JsonPathExtractor() throws JsonProcessingException {
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        t.setLabels(a1);
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
        t.setLabels(a1,found);

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
        t.setLabels(a1,found);
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
        t.setLabels(a1,found);
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
        t.setLabels(a1);
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
        t.setLabels(jenkinsBuild);
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
        t.setLabels(foundA,iterA,a1);
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
        t.setLabels(firstAKey,a1);
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
        t.setLabels(iterA,a1);
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        Run.persist(r);

        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        Label l = Label.find("from Label l where l.name=?1 and l.group.id=?2","iterA",t.id).firstResult();
        assertNotNull(l,"label should exist");
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.getByName(l.name).get(0).isIterated());
        assertEquals(1,extractedValues.getByName(l.name).size(),"unexpected number of entries in "+extractedValues.getByName(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.getByName(l.name).get(0).data(),"unexpected: "+extractedValues.getByName(l.name));
        assertEquals(3,extractedValues.getByName(l.name).get(0).data().size(),"unexpected number of entries in "+extractedValues.getByName(l.name)+"[0]");
    }

    @org.junit.jupiter.api.Test
    public void createLabelValues_nested() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, JsonProcessingException, NotSupportedException {
        tm.begin();
        Test t = new Test("nested");
        Label foo = new Label("fo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("fo"));
        Label iterFoo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("fo[]").setName("foo"))
                .setTargetSchema(new LabelGroup("foos"));
        Label bar = new Label("bar",t)
                .loadExtractors(Extractor.fromString("foo[]:$.bar").setName("bar"));
        Label biz = new Label("biz",t)
                .loadExtractors(Extractor.fromString("bar[]:$.biz").setName("biz"));
        Label sum = new Label("sum",t)
                .loadExtractors(
                        Extractor.fromString("biz[]:$.a").setName("a"),
                        Extractor.fromString("biz[]:$.b").setName("b")
                ).setReducer("({a,b})=>(a||'')+(b||'')");
        t.setLabels(foo,iterFoo,bar,biz,sum);
        t.persistAndFlush();
        Run r = new Run(
                t.id,
                new ObjectMapper().readTree("""
                {
                    "foo": [
                      {
                        "bar": [
                          {
                            "biz": [
                              { "a": "a00", "b": "b00"},
                              { "a": "a01"},
                              { "a": "a02", "b": "b02"}
                            ]
                          }
                        ]
                      },
                      {
                        "bar": [
                          {
                            "biz": [
                              { "a": "a10", "b": "b10"},
                              { "b": "b11"},
                              { "a": "a12", "b": "b12"}
                            ]
                          }
                        ]
                      }
                    ]
                }
                """),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,sum.id).list();

        assertEquals(6,lvs.size(),"expect 6 values for sum: "+lvs);
        Arrays.asList("a00b00","a01","a02b02","a10b10","b11","a12b12").forEach(v->{
            assertTrue(lvs.stream().anyMatch(lv->lv.data.isTextual() && v.equals(lv.data.asText())),"could not find "+v+" in "+lvs);
        });
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
        t.setLabels(foo,iterFoo,bar,iterBar,iterBarSum);
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
        LabelGroup direct = new LabelGroup("direct");
        Test t = new Test("target_schema");
        Label foo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("foo"));
        Label iterFoo = new Label("iterFoo",t)
                .loadExtractors(Extractor.fromString("foo[]").setName("iterFoo"))
                .setTargetSchema(direct);
        Label biz = new Label("biz",t)
                .loadExtractors(Extractor.fromString("iterFoo:$.biz").setName("biz"));
        Label buz = new Label("buz",t)
                .loadExtractors(Extractor.fromString("iterFoo:$.buz").setName("buz"));
        t.setLabels(foo,iterFoo,biz,buz);
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
        List<LabelService.ValueMap> valueMaps = labelService.labelValues(direct,t.id,Collections.emptyList(),Collections.emptyList());
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
        LabelGroup direct = new LabelGroup("direct");
        Test t = new Test("target_schema");
        Label foo = new Label("foo",t)
                .loadExtractors(Extractor.fromString("$.foo").setName("foo"))
                .setTargetSchema(direct);
        Label bar = new Label("bar",t)
                .loadExtractors(Extractor.fromString("$.bar").setName("bar"))
                .setTargetSchema(direct);
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
        t.setLabels(foo,fooBiz,bar,barBiz);
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
        List<LabelService.ValueMap> valueMaps = labelService.labelValues(direct,t.id,Collections.emptyList(),Collections.emptyList());
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
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label nxn = new Label("nxn",t)
                .loadExtractors(
                        Extractor.fromString("iterA").setName("foundA"),
                        Extractor.fromString("iterB").setName("foundB")
                );
        nxn.reducer = new LabelReducer("({foundA,foundB})=>foundA*foundB");
        nxn.multiType= Label.MultiIterationType.NxN;
        t.setLabels(a1,b1,iterA,iterB,nxn); // order should not matter
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
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterAKey = new Label("iterAKey",t)
                .setTargetSchema(new LabelGroup("uri:different:keyed"))
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("iterAKey"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
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

        t.setLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
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
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_NxN_jsonpath_on_iterated() throws JsonProcessingException {

        Test t = createTest();
        Run r = createRun(t,"uno");
        Label nxn = Label.find("from Label l where l.name=?1 and l.group.id=?2","nxn",t.id).firstResult();

        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,nxn.id).list();
        assertNotNull(lvs,"label_value should exit");
        //TODO this branch does not yet support NxN
        assertEquals(6,lvs.size(),"expect 6 entries:\n  "+lvs.stream().map(LabelValue::toString).collect(Collectors.joining("\n  ")));
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_alpha\"}"),lvs.get(0).data);
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_alpha\"}"),lvs.get(1).data);
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_alpha\"}"),lvs.get(2).data);
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_bravo\"}"),lvs.get(3).data);
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_bravo\"}"),lvs.get(4).data);
        assertEquals(new ObjectMapper().readTree("{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_bravo\"}"),lvs.get(5).data);
    }

    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath_on_iterated() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        Label found = Label.find("from Label l where l.name=?1 and l.group.id=?2","foundA",t.id).singleResult();
        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).list();
        assertNotNull(lvs,"label_value should exit");
        assertEquals(3,lvs.size(),lvs.toString());
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_NxN_reducing() throws JsonProcessingException {
        Test t = new Test("reducer-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        a1.reducer= new LabelReducer("ary=>ary.map(v=>v*2)");
        Label iterA = new Label("iterA",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("b1[]").setName("iterB"));
        Label nxn = new Label("nxn",t)
                .loadExtractors(
                        Extractor.fromString("iterA").setName("foundA"),
                        Extractor.fromString("iterB").setName("foundB")
                );
        nxn.reducer = new LabelReducer("({foundA,foundB})=>foundA*foundB");
        nxn.multiType= Label.MultiIterationType.NxN;
        t.setLabels(a1,b1,iterA,iterB,nxn); // order should not matter
        t.persist();
        Run r = new Run(t.id,
                new ObjectMapper().readTree("{ \"a1\":[0, 2, 4],\"b1\":[1, 3, 5]}"),new ObjectMapper().readTree("{}"));
        Run.persist(r);
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelValue> found = LabelValue.find("from LabelValue lv where  lv.label.name=?1 and lv.run.id=?2","nxn",r.id).list();
        assertEquals(9,found.size(),found.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));


        LabelValue lv = LabelValue.find("from LabelValue lv where lv.label.name=?1 and lv.run.id=?2","a1",r.id).singleResult();
        assertNotNull(lv);
        assertTrue(lv.data.isArray());
        assertEquals(3,lv.data.size());
        assertEquals(0,lv.data.get(0).intValue());
        assertEquals(4,lv.data.get(1).intValue());
        assertEquals(8,lv.data.get(2).intValue());
        //it applied the 2x function
        //assertEquals(new ObjectMapper().readTree("[0,4,8]"),lv.data);//why on earth does this not work, is it long vs int?
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
        t.setLabels(key);
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
        Label key = Label.find("from Label l where l.name=?1 and l.group.id=?2","key",t.id).singleResult();
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
        Label key = Label.find("from Label l where l.name=?1 and l.group.id=?2","key",t.id).singleResult();
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

    //not sure if we should support this use case (a label without extractors)
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_no_extractor() throws JsonProcessingException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Test t = new Test("no-extractor");
        Label l = new Label("foo",t)
                .setReducer("(v)=>{ return 'foo';}");
        t.setLabels(l);
        t.persist();
        Run r = new Run(t.id,new ObjectMapper().readTree("{\"biz\":\"buz\"}"),new ObjectMapper().readTree("{}"));
        r.persistAndFlush();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",l.id,r.id).list();
        assertEquals(1,lvs.size(),lvs.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));
        assertEquals("foo",lvs.get(0).data.asText());

    }

    @org.junit.jupiter.api.Test
    public void getDerivedValues_iterA() throws JsonProcessingException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        LabelGroup keyed  = new LabelGroup("uri:keyed");
        LabelGroup different = new LabelGroup("uri:different:keyed");
        Test t = new Test("example-test");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label firstAKey = new Label("firstAKey",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("firstAKey")); //this is grabbing the wrong a1 :(
        Label justA = new Label("justA",t)
                .loadExtractors(Extractor.fromString("a1").setName("justA"));
        Label iterA = new Label("iterA",t)
                .setTargetSchema(keyed)
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterAKey = new Label("iterAKey",t)
                .setTargetSchema(different)
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("iterAKey"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(keyed)
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
        t.setLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
        t.persist();
        Run r1 = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"b1\": [{\"key\":\"b1_alpha\"}, {\"key\":\"b1_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"1\" } }"));
        Run r2 = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}, {\"key\":\"a2_charlie\"}], \"b1\": [{\"key\":\"b2_alpha\"}, {\"key\":\"b2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"1\" } }"));
        r1.persist();
        r2.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r1.id);
        labelService.calculateLabelValues(t.labels,r2.id);
        List<LabelValue> labelValues = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",iterA.id,r1.id).list();
        assertEquals(3,labelValues.size(),labelValues.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));
        List<LabelValue> found = labelService.getDerivedValues(labelValues.get(0),0);
        assertFalse(found.isEmpty(),"found should not be empty:\n"+found.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));
        assertEquals(3,found.size(),found.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));
        assertTrue(found.stream().anyMatch(lv->lv.label.equals(nxn)),found.stream().map(lv->lv.toString()+" "+lv.label.name).collect(Collectors.joining("\n")));
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
        t.setLabels(found);
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
    public void labelValues_nested_3_deep() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        LabelGroup group = new LabelGroup("schema");
        Test t = new Test("deep");
        t.persistAndFlush();
        Label foo = new Label("foo",t).loadExtractors(Extractor.fromString("$.foo").setName("foo"));
        Label schema = new Label("schema",t).loadExtractors(Extractor.fromString("foo[]").setName("schema"))
                .setTargetSchema(group);
        Label b1 = new Label("b1",t).loadExtractors(Extractor.fromString("schema:$.b1").setName("b1"));
        Label b2 = new Label("b2",t).loadExtractors(Extractor.fromString("b1:$.b2").setName("b2"));
        Label b3 = new Label("b3",t).loadExtractors(Extractor.fromString("b2:$.b3").setName("b3"));
        Label a1 = new Label("a1",t).loadExtractors(Extractor.fromString("schema:$.a1").setName("a1"));
        Label a2 = new Label("a2",t).loadExtractors(Extractor.fromString("a1:$.a2").setName("a2"));
        Label a3 = new Label("a3",t).loadExtractors(Extractor.fromString("a2:$.a3").setName("a3"));
        Label c1 = new Label("c1",t).loadExtractors(Extractor.fromString("schema:$.c1").setName("c1"));
        Label c2 = new Label("c2",t).loadExtractors(Extractor.fromString("c1:$.c2").setName("c2"));
        Label c3 = new Label("c3",t).loadExtractors(Extractor.fromString("c2:$.c3").setName("c3"));
        t.setLabels(foo,schema,b1,b2,b3,a1,a2,a3,c1,c2,c3);
        Run r = new Run(t.id,
                new ObjectMapper().readTree("""
                    {"foo": [
                        {"a1":{"a2":{"a3":"a_first"}}, "b1":{"b2":{"b3":"b_first"}}, "c1":{"c2":{"c3":"c_first"}} },
                        {"a1":{"a2":{"a3":"a_second"}}, "b1":{"b2":{"b3":"b_second"}}, "c1":{"c2":{}} },
                        {"a1":{"a2":{"a3":"a_third"}}, "b1":{"b2":{"b3":"b_third"}}, "c1":{"c2":{"c3":"c_third"}} }
                    ]}
                """),
                new ObjectMapper().readTree("{}")
        );
        r.persistAndFlush();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelService.ValueMap> valueMaps = labelService.labelValues(group,t.id,Arrays.asList("a3","b3","c3"),null);
        assertEquals(3,valueMaps.size());
        assertEquals("c_first",valueMaps.get(0).data().get("c3").asText());
        assertFalse(valueMaps.get(1).data().has("c3"));
        assertEquals("c_third",valueMaps.get(2).data().get("c3").asText());
    }

    @org.junit.jupiter.api.Test
    public void labelValues_schema() throws JsonProcessingException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Test t = new Test("example-test");
        LabelGroup keyed = new LabelGroup("uri:keyed");
        LabelGroup different = new LabelGroup("uri:different:keyed");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label firstAKey = new Label("firstAKey",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("firstAKey"));
        Label justA = new Label("justA",t)
                .loadExtractors(Extractor.fromString("a1").setName("justA"));
        Label iterA = new Label("iterA",t)
                .setTargetSchema(keyed)
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterAKey = new Label("iterAKey",t)
                .setTargetSchema(different)
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("iterAKey"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(keyed)
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

        t.setLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
        t.persist();
        Run r1 = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"b1\": [{\"key\":\"b1_alpha\"}, {\"key\":\"b1_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"1\" } }"));
        r1.persist();
        tm.commit();
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        List<LabelService.ValueMap> labelValues = labelService.labelValues(keyed,t.id, Collections.emptyList(),Collections.emptyList());
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
        labelService.calculateLabelValues(t.labels, r1.id);
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
    public void labelValues_label() throws JsonProcessingException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Test t = new Test("labelValues_label");
        Label a1 = new Label("a1",t)
                .loadExtractors(Extractor.fromString("$.a1").setName("a1"));
        Label b1 = new Label("b1",t)
                .loadExtractors(Extractor.fromString("$.b1").setName("b1"));
        Label firstAKey = new Label("firstAKey",t)
                .loadExtractors(Extractor.fromString("a1:$[0].key").setName("firstAKey"));
        Label justA = new Label("justA",t)
                .loadExtractors(Extractor.fromString("a1").setName("justA"));
        Label iterA = new Label("iterA",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
                .loadExtractors(Extractor.fromString("a1[]").setName("iterA"));
        Label iterAKey = new Label("iterAKey",t)
                .setTargetSchema(new LabelGroup("uri:different:keyed"))
                .loadExtractors(Extractor.fromString("a1[]:$.key").setName("iterAKey"));
        Label iterB = new Label("iterB",t)
                .setTargetSchema(new LabelGroup("uri:keyed"))
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
        t.setLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
        t.persistAndFlush();

        assertEquals(11,t.labels.size(),"expect test to have 11 labels");

        Run r1 = new Run(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"b1\": [{\"key\":\"b1_alpha\"}, {\"key\":\"b1_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"1\" } }"));
        r1.persist();
        tm.commit();
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels, r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        Label l = Label.find("from Label l where l.name=?1 and l.group.id=?2",iterA.name,t.id).singleResult();
        List<LabelService.ValueMap> labelValues = labelService.labelValues(l.id, r1.id,t.id, Collections.emptyList(),Collections.emptyList());
        assertEquals(6,labelValues.size(),labelValues.stream().map(LabelService.ValueMap::toString).collect(Collectors.joining("\n")));
    }
    @org.junit.jupiter.api.Test
    public void multiType_NxN() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels, r1.id);
        Label l = Label.find("from Label l where l.name=?1 and l.group.id=?2","nxn",t.id).singleResult();
        List<LabelValue> lvs = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r1.id,l.id).list();

        //expect 6 entries given 3 from iterA and 2 from iterB
        assertEquals(6,lvs.size(),lvs.stream().map(LabelValue::toString).collect(Collectors.joining("\n")));
        //make sure the data contains the proper 3 x 2 combinations
    }

    @org.junit.jupiter.api.Test
    public void getDescendantLabels() throws JsonProcessingException {
        Test t = createTest();

        Label a1 = Label.find("from Label l where l.name=?1 and l.group.id=?2","a1",t.id).singleResult();
        Label firstAKey = Label.find("from Label l where l.name=?1 and l.group.id=?2","firstAKey",t.id).singleResult();
        Label justA = Label.find("from Label l where l.name=?1 and l.group.id=?2","justA",t.id).singleResult();
        Label iterA = Label.find("from Label l where l.name=?1 and l.group.id=?2","iterA",t.id).singleResult();
        Label iterAKey = Label.find("from Label l where l.name=?1 and l.group.id=?2","iterAKey",t.id).singleResult();
        Label foundA = Label.find("from Label l where l.name=?1 and l.group.id=?2","foundA",t.id).singleResult();
        Label nxn = Label.find("from Label l where l.name=?1 and l.group.id=?2","nxn",t.id).singleResult();

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
