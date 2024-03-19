package io.hyperfoil.tools.exp.horreum.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

@Entity
@Table(name = "lv_ref")
public class LvRef extends PanacheEntityBase {

    @Id
    public Long index; //the index in the target label_value

    @Id
    @ManyToOne(cascade = CascadeType.PERSIST,fetch = FetchType.EAGER)
    @JoinColumn(name="label_id", referencedColumnName = "label_id")
    @JoinColumn(name="run_id", referencedColumnName = "run_id")
    public LabelValue target;


    public boolean isValid(){
        return target.data.isArray() && target.data.size() > index && index > -1;
    }
}
