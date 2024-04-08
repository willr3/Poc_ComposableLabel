package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class JsonpathExtractorTest {

    @org.junit.jupiter.api.Test
    public void fromString_jsonpath(){
        JsonpathExtractor ex = JsonpathExtractor.fromString(JsonpathExtractor.PREFIX+".foo.bar");
        assertNotNull(ex);
        assertEquals((JsonpathExtractor.PREFIX+".foo.bar"),ex.jsonpath,"unexpected jsonpath");
    }
    @org.junit.jupiter.api.Test
    public void fromString_iterate_jsonpath(){
        JsonpathExtractor ex = JsonpathExtractor.fromString(LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR+JsonpathExtractor.PREFIX+".foo.bar");
        assertNotNull(ex);
        assertEquals((JsonpathExtractor.PREFIX+".foo.bar"),ex.jsonpath,"unexpected jsonpath");
        assertTrue(ex.forEach,"ex should iterate");
    }
}
