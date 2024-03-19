package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.smallrye.common.constraint.NotNull;
import jakarta.json.Json;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * Extract from the run data using jsonpaths
 */
@Entity
/*@Table(name = "extractor")*/
public class JsonpathExtractor extends Extractor{

    public static final String PREFIX="$";
    //public static final String FOR_EACH_SUFFIX="[*]";
    @NotNull
    public String jsonpath;
    @Override
    public String toString(){
        return jsonpath;
    }

    public static JsonpathExtractor fromString(String input){
        JsonpathExtractor rtrn = null;
        if(input!=null && !input.isBlank()){
            rtrn = new JsonpathExtractor();
            rtrn.name = generateName();
            if(input.startsWith(LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR)){
                rtrn.forEach=true;
                input = input.substring(LabelValueExtractor.FOR_EACH_SUFFIX.length()+LabelValueExtractor.NAME_SEPARATOR.length());
            }
            rtrn.jsonpath=input;
        }
        return rtrn;
    }
}
