package io.hyperfoil.tools.exp.horreum.entity;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.exp.horreum.pasted.JsonBinaryType;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.util.List;

@Entity
@Table(name = "label_values",uniqueConstraints = {
        @UniqueConstraint(columnNames = {"run_id","label_id"})
})
public class LabelValue extends PanacheEntityBase {

    public boolean iterated; //if the value contains an array that represents the result of

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "label_value_references")
    @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    public List<LvReferences> references;

    @Id
    @ManyToOne
    @JoinColumn(name="run_id")
    public Run run;

    @Id
    @ManyToOne
    @JoinColumn(name="label_id")
    public Label label;
    @Type(JsonBinaryType.class)
    public JsonNode data;
}
