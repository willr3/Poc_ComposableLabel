package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.svc.LabelService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LabelValueExtractorTest {

    @Transactional
    @org.junit.jupiter.api.Test
    public void persist_invalid_targetLabel(){
        Extractor lve = Extractor.fromString("name"+Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".foo.bar");
        try{
            lve.persistAndFlush();
            fail("should not be able to persist");
        }catch (Exception ignored){}
    }

    @org.junit.jupiter.api.Test
    public void fromString_justName(){
        Extractor lve = Extractor.fromString("name");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue("name".equals(lve.targetLabel.name));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate(){
        Extractor lve = Extractor.fromString("name"+Extractor.FOR_EACH_SUFFIX);
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate_separator(){
        Extractor lve = Extractor.fromString("name"+Extractor.FOR_EACH_SUFFIX+Extractor.NAME_SEPARATOR);
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
    }

    @org.junit.jupiter.api.Test
    public void fromString_name_separator_jsonpath(){
        Extractor lve = Extractor.fromString("name"+Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".foo.bar");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue("name".equals(lve.targetLabel.name));
        assertTrue((Extractor.PREFIX+".foo.bar").equals(lve.jsonpath));
    }
    @org.junit.jupiter.api.Test
    public void fromString_name_iterate_separator_jsonpath(){
        Extractor lve = Extractor.fromString("name"+Extractor.FOR_EACH_SUFFIX+Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".foo.bar");
        assertNotNull(lve.targetLabel,"targetLabel should not be null");
        assertTrue(lve.forEach,"lve should be iterating");
        assertTrue("name".equals(lve.targetLabel.name));
        assertTrue((Extractor.PREFIX+".foo.bar").equals(lve.jsonpath));
    }
}
