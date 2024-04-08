package io.hyperfoil.tools.exp.horreum.entity;

import jakarta.inject.Inject;
import jakarta.persistence.*;

@Entity
@Table(
    name = "label_value_pointer",
    //this is a lot of indexes, are they really necessary?
    indexes = {
            @Index(name="lvp_child_label_id",columnList = "child_label_id",unique = false),
            @Index(name="lvp_child_run_id",columnList = "child_run_id",unique = false),
            @Index(name="lvp_target_label_id",columnList = "target_label_id",unique = false)
    }
)
public class LabelValuePointer {

    @Id
    public Long childIndex;

    @Id
    @ManyToOne(cascade = CascadeType.PERSIST,fetch = FetchType.EAGER)
    @JoinColumn(name="child_label_id", referencedColumnName = "label_id")
    @JoinColumn(name="child_run_id", referencedColumnName = "run_id")
    public LabelValue child;

    @Id
    public Long targetIndex;

    @Id
    @ManyToOne(cascade = CascadeType.PERSIST,fetch = FetchType.EAGER)
    @JoinColumn(name="target_label_id", referencedColumnName = "label_id")
    @JoinColumn(name="target_run_id", referencedColumnName = "run_id")
    public LabelValue target;

    public static LabelValuePointer create(long childIndex,LabelValue child,long targetIndex, LabelValue target){
        LabelValuePointer rtrn = new LabelValuePointer();
        rtrn.child = child;
        rtrn.childIndex = childIndex;
        rtrn.target = target;
        rtrn.targetIndex = targetIndex;
        return rtrn;
    }

    @Override
    public String toString(){
        return childIndex+" -> target=[run_id="+target.run.id+" label_id="+target.label.id+" index="+targetIndex+"]";
    }
}
