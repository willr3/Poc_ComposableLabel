package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class JsonpathExtractorTest {

    @org.junit.jupiter.api.Test
    public void fromString_jsonpath(){
        Extractor ex = Extractor.fromString(Extractor.PREFIX+".foo.bar");
        assertNotNull(ex);
        assertEquals((Extractor.PREFIX+".foo.bar"),ex.jsonpath,"unexpected jsonpath");
    }
    @org.junit.jupiter.api.Test
    public void fromString_iterate_jsonpath(){
        Extractor ex = Extractor.fromString(Extractor.FOR_EACH_SUFFIX+Extractor.NAME_SEPARATOR+Extractor.PREFIX+".foo.bar");
        assertNotNull(ex);
        assertEquals((Extractor.PREFIX+".foo.bar"),ex.jsonpath,"unexpected jsonpath");
        assertTrue(ex.forEach,"ex should iterate");
    }
}
