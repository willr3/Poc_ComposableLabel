package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class LabelValueExtractorTest {

    @org.junit.jupiter.api.Test
    public void fromString_justName(){
        LabelValueExtractor lve = LabelValueExtractor.fromString("name");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue("name".equals(lve.targetLabel.name));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate(){
        LabelValueExtractor lve = LabelValueExtractor.fromString("name"+LabelValueExtractor.FOR_EACH_SUFFIX);
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate_separator(){
        LabelValueExtractor lve = LabelValueExtractor.fromString("name"+LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR);
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
    }

    @org.junit.jupiter.api.Test
    public void fromString_name_separator_jsonpath(){
        LabelValueExtractor lve = LabelValueExtractor.fromString("name"+LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".foo.bar");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue("name".equals(lve.targetLabel.name));
        assertTrue((JsonpathExtractor.PREFIX+".foo.bar").equals(lve.jsonpath));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate_separator_jsonpath(){
        LabelValueExtractor lve = LabelValueExtractor.fromString("name"+LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".foo.bar");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
        assertTrue((JsonpathExtractor.PREFIX+".foo.bar").equals(lve.jsonpath));
    }
}
