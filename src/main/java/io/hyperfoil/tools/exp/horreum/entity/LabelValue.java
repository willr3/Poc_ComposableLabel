package io.hyperfoil.tools.exp.horreum.entity;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.exp.horreum.pasted.JsonBinaryType;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Entity
@Table(
    name = "label_values",
    //indexes used by LabelService.calculateExtractedValuesWithIterated
    indexes = {
        @Index(name="lv_run_id",columnList = "run_id",unique = false),
        @Index(name="lv_label_id",columnList = "label_id",unique = false)
    }
)
public class LabelValue extends PanacheEntity {

    //@ElementCollection(fetch = FetchType.EAGER)
    @ManyToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER )
    @JoinTable(name = "label_value_sources",uniqueConstraints = {})
    public Set<LabelValue> sources = new HashSet<>();//what label_values were used to create this label_value

    @ManyToOne
    @JoinColumn(name="run_id")
    public Run run;

    @ManyToOne
    @JoinColumn(name="label_id")
    public Label label;

    @Type(JsonBinaryType.class)
    public JsonNode data;

    public int ordinal;//used to keep extracted values together when coming from an iterated source

    public void addSource(LabelValue source){
        sources.add(source);
    }
    public void removeSource(LabelValue source){
        sources.remove(source);
    }

    @Override
    public String toString(){
        return String.format("LabelValue[id=%d, run_id=%d, label_id=%d, ordinal=%d, data=%s]",id,run.id,label.id,ordinal,data==null ? "null":data.toString());
    }

    @Override
    public int hashCode(){
        return Objects.hash(id);
    }
    @Override
    public boolean equals(Object object){
        return object instanceof LabelValue &&
                this.run.equals(((LabelValue)object).run) &&
                this.label.equals(((LabelValue)object).label);
    }
}
