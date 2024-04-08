package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.exp.horreum.entity.*;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.RunMetadataExtractor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Disabled;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LabelServiceTest {
    @Inject
    EntityManager em;
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
        assertNotNull(lv,"label_value should exit");
        assertInstanceOf(ArrayNode.class,lv.data);
        ArrayNode arrayNode = (ArrayNode)lv.data;
        assertEquals(3,arrayNode.size(),arrayNode.toString());
    }

    //case when m.dtype = 'JsonpathExtractor' and m.jsonpath is not null
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_JsonpathExtractor_jsonpath() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);

        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","a1",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertTrue(extractedValues.hasNonNull(l.name));
        assertFalse(extractedValues.isIterated(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.get(l.name));
    }
    //case when m.dtype = 'RunMetadataExtractor' and m.jsonpath is not null and m.column_name = 'metadata'
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_RunMetadataExtractor_jsonpath() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","build",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertTrue(extractedValues.hasNonNull(l.name));
        assertFalse(extractedValues.isIterated(l.name));
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.get(l.name));
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.lv_iterated
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_iterated_jsonpath() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.isIterated(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.get(l.name),"unexpected: "+extractedValues.get(l.name));
        ArrayNode arrayNode = (ArrayNode) extractedValues.get(l.name);
        assertEquals(3,arrayNode.size(),"unexpected number of entries in "+arrayNode);
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != ''
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_jsonpath() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","firstAKey",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.isIterated(l.name));
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.get(l.name),"unexpected: "+extractedValues.get(l.name));
        assertEquals("a1_alpha",((TextNode)extractedValues.get(l.name)).asText());
    }
    //case when m.dtype = 'LabelValueExtractor' and (m.jsonpath is null or m.jsonpath = '')
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_no_jsonpath() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).firstResult();
        LabelService.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.isIterated(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.get(l.name),"unexpected: "+extractedValues.get(l.name));
        ArrayNode arrayNode = (ArrayNode) extractedValues.get(l.name);
        assertEquals(3,arrayNode.size(),"unexpected number of entries in "+arrayNode);
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
        t.persist();
        return t;
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
        Label nxn = Label.find("from Label l where l.name=?1 and l.parent.id=?2","nxn",t.id).firstResult();

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,nxn.id).firstResult();
        assertNotNull(lv,"label_value should exit");
        assertInstanceOf(ArrayNode.class,lv.data,"unexpected data type: "+lv);
        ArrayNode arrayNode = (ArrayNode)lv.data;
        assertEquals(6,arrayNode.size(),"expect 6 entries: "+arrayNode);
        JsonNode expected = (new ObjectMapper()).readTree("[{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_bravo\"}]");
        assertEquals(expected,lv.data,"data did not match expected "+lv.data);
    }

    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath_on_iterated() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        Label found = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        LabelValue lv = LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).singleResult();
        assertNotNull(lv,"label_value should exit");
        assertInstanceOf(ArrayNode.class,lv.data);
        ArrayNode arrayNode = (ArrayNode)lv.data;
        assertEquals(3,arrayNode.size(),arrayNode.toString());
    }


    @Disabled //this is not a good unit test :)
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_degradation_test() throws JsonProcessingException {
        Test t = createTest();
        int LIMIT = 50;
        long length = Math.round(Math.ceil(Math.log10(LIMIT)));
        for(int i=0; i< LIMIT; i++){
            Run r = createRun(t,String.format("%"+length+"d",i));
            long start = System.currentTimeMillis();
            labelService.calculateLabelValues(t.labels,r.id);
            long stop = System.currentTimeMillis();
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
        JsonNode node = (new ObjectMapper()).readTree(data);
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
        LabelValue labelValue = LabelValue.find("from LabelValue lv where lv.label.id=?1 and lv.run.id=?2",iterA.id,r1.id).singleResult();
        List<LabelValue> found = labelService.getDerivedValues(labelValue,0);
        assertFalse(found.isEmpty(),"found should not be empty");
        assertEquals(2,found.size());
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
        assertEquals(2,found.size(),"found should have 2 entries: "+found);
    }

    @org.junit.jupiter.api.Test
    public void usesIterated_direct() throws JsonProcessingException {
        Test t = createTest();
        Run r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        Label found = Label.find("from Label l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        Label iter = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        Label a1 = Label.find("from Label l where l.name=?1 and l.parent.id=?2","a1",t.id).singleResult();

        assertTrue(labelService.usesIterated(r.id,found.id),"found uses an iterated label_value");
        assertFalse(labelService.usesIterated(r.id,a1.id),"a1 does not use a label_value");
        assertFalse(labelService.usesIterated(r.id,iter.id),"iterA does not use an iterated label_value");
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
    public void labelValues_label() throws JsonProcessingException {
        Test t = createTest();
        Run r1 = createRun(t,"uno");
        //Run r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        Label l = Label.find("from Label l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        List<LabelService.ValueMap> labelValues = labelService.labelValues(l.id, r1.id,t.id, Collections.emptyList(),Collections.emptyList());
        assertEquals(3,labelValues.size());
    }
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
