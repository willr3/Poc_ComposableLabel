package io.hyperfoil.tools.exp.horreum.entity;

import jakarta.persistence.*;

import java.util.Set;

@Entity
@Table(name = "lv_references")
public class LvReferences {

    @Id
    public Long index; //the index in parent
    @Id
    @ManyToOne
    @JoinColumn(name = "label_id",referencedColumnName = "label_id")
    @JoinColumn(name = "run_id",referencedColumnName = "run_id")
    public LabelValue parent; //the parent labelValue

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "lv_references_refs")
    @ManyToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    public Set<LvRef> references;
}
