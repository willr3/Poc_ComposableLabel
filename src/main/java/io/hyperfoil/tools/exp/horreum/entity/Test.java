package io.hyperfoil.tools.exp.horreum.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;

import java.util.Arrays;
import java.util.List;

@Entity
public class Test extends PanacheEntity {

    public String name;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "test_labels")
    @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    //Tried ManyToMany but it resulted in Labels not cascading persist
    public List<Label> labels;

    public Test(){}
    public Test(String name){
        this.name = name;
    }
    public Test loadLabels(Label...definitions){
        this.labels = Arrays.asList(definitions);
        return this;
    }
}
