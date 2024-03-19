package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

/**
 * Extract from a label value using jsonpath
 */
@Entity
/*@Table(name = "extractor")*/
public class LabelValueExtractor extends Extractor{

    public static final String FOR_EACH_SUFFIX = "[]";
    public static final String NAME_SEPARATOR=":";

    /** The id for the label that produces the value this extractor reads **/
    //@NotNull(message="invalid label reference")//is this breaking other extractors?
    @ManyToOne
    @JoinColumn(name="target_id")
    public Label targetLabel;
    /** Jsonpath in the same style */
    public String jsonpath="";

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder(targetLabel.name);
        if(forEach){
            sb.append("[]");
        }
        if(jsonpath!=null && !jsonpath.isBlank()){
            sb.append(":");
            sb.append(jsonpath);
        }
        return sb.toString();
    }
    public static LabelValueExtractor fromString(String input){
        LabelValueExtractor rtrn = null;
        if(input!=null && !input.isBlank()){
            rtrn = new LabelValueExtractor();
            rtrn.name = generateName();
            String name = input;
            if(input.contains(NAME_SEPARATOR)){
                name = input.substring(0,input.indexOf(NAME_SEPARATOR));
                rtrn.jsonpath = input.substring(input.indexOf(NAME_SEPARATOR)+NAME_SEPARATOR.length());
            }
            if(name.endsWith(FOR_EACH_SUFFIX)){
                rtrn.forEach=true;
                name = name.substring(0,name.length()-FOR_EACH_SUFFIX.length());
            }
            Label found = Label.find("name",name).firstResult();
//            Label found = null;
            if(found==null){
                found = new Label();
                found.name = name;
                System.out.println("CREATING A BLASTEd LABEL REF FOR "+name);
                //not sure if persisting is necessary, does this assume the new LabelValueExtractor is persisted too?
                //Label.persist(found);//need some association so that label.name is only unique to it's context (test or schema)
            }
            rtrn.targetLabel = found;//either a new entity or null
        }
        return rtrn;
    }
}
