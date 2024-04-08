package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.Run;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
/*@Table(name = "extractor")*/
public class RunMetadataExtractor extends Extractor{
    public static final String PREFIX = "{";
    public static final String SUFFIX = "}";
    public String column_name; //eventually support more than just metadata
    public String jsonpath;


    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(PREFIX);
        sb.append(column_name);
        sb.append(SUFFIX);
        if(jsonpath!=null && !jsonpath.isBlank()){
            sb.append(LabelValueExtractor.NAME_SEPARATOR);
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
            rtrn.column_name = name;
            input = input.substring(input.indexOf(SUFFIX)+SUFFIX.length());
            if(input.startsWith(LabelValueExtractor.FOR_EACH_SUFFIX)){
                rtrn.forEach = true;
                input = input.substring(LabelValueExtractor.FOR_EACH_SUFFIX.length());
            }
            if(input.startsWith(LabelValueExtractor.NAME_SEPARATOR)){
                input=input.substring(LabelValueExtractor.NAME_SEPARATOR.length());
            }
            if(input.startsWith(JsonpathExtractor.PREFIX)){
                rtrn.jsonpath = input;
            }
        }
        return rtrn;
    }

}
