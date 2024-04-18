package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest

public class RunMetadataExtractorTest {

    @org.junit.jupiter.api.Test
    public void fromString_column(){
        String str = Extractor.METADATA_PREFIX+"id"+Extractor.METADATA_SUFFIX;
        Extractor ex = Extractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("id",ex.column_name,"incorrect column name");
    }
    @org.junit.jupiter.api.Test
    public void fromString_column_iterate(){
        String str = Extractor.METADATA_PREFIX+"metadata"+Extractor.METADATA_SUFFIX+Extractor.FOR_EACH_SUFFIX;
        Extractor ex = Extractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertTrue(ex.forEach,"should be an iterating extractor");
    }

    @org.junit.jupiter.api.Test
    public void fromString_column_jsonpath(){
        String str = Extractor.METADATA_PREFIX+"metadata"+Extractor.METADATA_SUFFIX+Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".foo.bar";
        Extractor ex = Extractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertEquals(Extractor.PREFIX+".foo.bar",ex.jsonpath,"incorrect jsonpath");
    }
    @org.junit.jupiter.api.Test
    public void fromString_column_iterate_jsonpath(){
        String str = Extractor.METADATA_PREFIX+"metadata"+Extractor.METADATA_SUFFIX+Extractor.FOR_EACH_SUFFIX+Extractor.NAME_SEPARATOR+ Extractor.PREFIX+".foo.bar";
        Extractor ex = Extractor.fromString(str);
        assertNotNull(ex,str+" is a valid extractor");
        assertEquals("metadata",ex.column_name,"incorrect column name");
        assertTrue(ex.forEach,"should be an iterating extractor");
        assertEquals(Extractor.PREFIX+".foo.bar",ex.jsonpath,"incorrect jsonpath");
    }


}
