package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.Run;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
/*@Table(name = "extractor")*/
public class RunMetadataExtractor extends Extractor{
    public static final String PREFIX = "{";
    public static final String SUFFIX = "}";
    public static final String SEPARATOR= ":";

    public String columnName;
    public String jsonpath;


    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(columnName);
        sb.append("}");
        if(jsonpath!=null && !jsonpath.isBlank()){
            sb.append(":");
            sb.append(jsonpath);
        }
        return sb.toString();
    }

    public static RunMetadataExtractor fromString(String input){
        RunMetadataExtractor rtrn = null;
        if(input!=null && !input.isBlank() && input.startsWith(PREFIX) && input.contains(SUFFIX)){
            rtrn = new RunMetadataExtractor();
            rtrn.jsonpath=input;
            String name = input.substring(PREFIX.length(),input.indexOf(SUFFIX));
            rtrn = new RunMetadataExtractor();
            rtrn.name = generateName();
            rtrn.columnName = name;
            input = input.substring(input.indexOf(SUFFIX)+SUFFIX.length());
            if(input.startsWith(SEPARATOR)){
                input=input.substring(SEPARATOR.length());
            }
            if(input.startsWith(JsonpathExtractor.PREFIX)){
                rtrn.jsonpath = input;
            }
        }
        return rtrn;
    }

}
