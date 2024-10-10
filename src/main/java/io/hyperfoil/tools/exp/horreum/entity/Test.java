package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.valid.ValidLabel;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Entity
@DiscriminatorValue("test")
public class Test extends LabelGroup {
    public Test(){}
    public Test(String name){
        super(name);
    }
    public Test setLabels(Label...labels){
        if(this.labels == null || this.labels.isEmpty()){
            this.labels = new ArrayList<>(Arrays.asList(labels));
            checkLabels();
        }
        return this;
    }
    public boolean equals(Object o){
        if(o == null || !(o instanceof Test)){
            return false;
        }
        Test t = (Test)o;
        return ((this.id == null || t.id == null) && this.name.equals(t.name)) ||  (this.id != null && this.id.equals(t.id));
    }
}
