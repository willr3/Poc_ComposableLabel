package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.annotation.CheckLabelValueExtractors;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Entity
@CheckLabelValueExtractors
public class Label extends PanacheEntity implements Comparable<Label> {

    public static enum MultiIterationType { Length, NxN}
    public static enum ScalarVariableMethod { First, All}
    
    public String name;

    @ElementCollection(fetch = FetchType.EAGER)
    @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER, orphanRemoval = true, mappedBy = "parent")

    public List<@NotNull(message="wtf mate") Extractor> extractors;
    @ManyToOne(cascade = CascadeType.PERSIST)
    public LabelReducer reducer;

    @Enumerated(EnumType.STRING)
    public MultiIterationType multiType = MultiIterationType.Length;
    @Enumerated(EnumType.STRING)
    public ScalarVariableMethod scalarMethod = ScalarVariableMethod.First;

    public Label(){}
    public Label(String name){
        this.name = name;
    }

    public Label loadExtractors(Extractor...extractors){
        this.extractors = Arrays.asList(extractors);
        this.extractors.forEach(e->e.parent=this);
        return this;
    }

    @Override
    public String toString(){return name+"="+id;}


    @Override
    public int compareTo(Label o1) {
        if(o1.usesLabelValueExtractor() && !this.usesLabelValueExtractor()){
            return -1;//o1 is less than 02
        }else if (this.usesLabelValueExtractor() && !o1.usesLabelValueExtractor()){
            return 1;//o2 is less than o1
        }else if (o1.dependsOn(this)){
            return -1;//o1 has to come after o2
        }else if (this.dependsOn(o1)){
            return 1;//o1 must come before o2
        }else{
            int nameDiff = o1.name.compareTo(this.name);
            if(nameDiff==0 && o1.id!=null && this.id!=null){
                return Long.compare(o1.id,this.id);
            }else{
                return nameDiff;
            }
        }
    }

    public long forEachCount(){
        return extractors.stream().filter(e->e.forEach).count();
    }
    public boolean hasForEach(){
        return extractors.stream().anyMatch(e->e.forEach);
    }
    public boolean usesOnlyJsonpathExtractor(){
        return extractors.stream().allMatch(e->e instanceof JsonpathExtractor);
    }
    public boolean usesLabelValueExtractor(){
        return extractors.stream().anyMatch(e->e instanceof LabelValueExtractor);
    }
    public boolean dependsOn(Label l){
        //do not replace id == l.id with .equals because id can be null
        return extractors.stream().anyMatch(e-> e instanceof LabelValueExtractor && ( (LabelValueExtractor)e).targetLabel.id == l.id && ( (LabelValueExtractor)e).targetLabel.name.equals(l.name));
    }


    /**
     * returns true if this is part of a circular reference
     * @return
     */
    public boolean isCircular(){
        if(!usesLabelValueExtractor()){
            return false;
        }
        Queue<Label> todo = new PriorityQueue<>();
        extractors.stream()
                .filter(e->e instanceof LabelValueExtractor)
                .map(e->((LabelValueExtractor) e).targetLabel).forEach(todo::add);
        Label target;
        boolean ok = true;
        while( ok && (target = todo.poll()) !=null ){
            if(this.equals(target)){
                ok = false;
            }
            List<Label> targetLabels = target.extractors.stream()
                    .filter(e->e instanceof LabelValueExtractor)
                    .map(e->((LabelValueExtractor) e).targetLabel)
                    .toList();
            todo.addAll(targetLabels);
        }
        return !ok;
    }
}
