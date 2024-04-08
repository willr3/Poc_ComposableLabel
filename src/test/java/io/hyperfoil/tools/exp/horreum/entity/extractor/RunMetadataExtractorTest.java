package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.RunMetadataExtractor;
import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest

public class RunMetadataExtractorTest {

    @org.junit.jupiter.api.Test
    public void fromString_column(){
        String str = RunMetadataExtractor.PREFIX+"id"+RunMetadataExtractor.SUFFIX;
        RunMetadataExtractor ex = RunMetadataExtractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("id",ex.column_name,"incorrect column name");
    }
    @org.junit.jupiter.api.Test
    public void fromString_column_iterate(){
        String str = RunMetadataExtractor.PREFIX+"metadata"+RunMetadataExtractor.SUFFIX+LabelValueExtractor.FOR_EACH_SUFFIX;
        RunMetadataExtractor ex = RunMetadataExtractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertTrue(ex.forEach,"should be an iterating extractor");
    }

    @org.junit.jupiter.api.Test
    public void fromString_column_jsonpath(){
        String str = RunMetadataExtractor.PREFIX+"metadata"+RunMetadataExtractor.SUFFIX+LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".foo.bar";
        RunMetadataExtractor ex = RunMetadataExtractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertEquals(JsonpathExtractor.PREFIX+".foo.bar",ex.jsonpath,"incorrect jsonpath");
    }
    @org.junit.jupiter.api.Test
    public void fromString_column_iterate_jsonpath(){
        String str = RunMetadataExtractor.PREFIX+"metadata"+RunMetadataExtractor.SUFFIX+LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR+ JsonpathExtractor.PREFIX+".foo.bar";
        RunMetadataExtractor ex = RunMetadataExtractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertTrue(ex.forEach,"should be an iterating extractor");
        assertEquals(JsonpathExtractor.PREFIX+".foo.bar",ex.jsonpath,"incorrect jsonpath");
    }


}
