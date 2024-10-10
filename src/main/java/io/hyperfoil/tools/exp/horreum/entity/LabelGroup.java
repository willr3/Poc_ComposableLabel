package io.hyperfoil.tools.exp.horreum.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.valid.ValidLabel;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type")
@DiscriminatorValue("group")
public class LabelGroup extends PanacheEntity implements Comparable<LabelGroup> {

    @JsonIgnore
    public String owner; //PoC stub for the Horreum owner concept

    @NotNull
    @Column(name="name", unique=false)
    public String name;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "group_labels")
    @OneToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}, fetch = FetchType.EAGER, orphanRemoval = false, mappedBy = "group")
    public List<@NotNull(message="null labels are not supported") @ValidLabel Label> labels;

    public LabelGroup(){}
    public LabelGroup(String name){
        this.name = name;
        this.labels = new ArrayList<>();
    }

    public int size(){
        return labels.size();
    }
    public boolean canLoad(LabelGroup group){
        Set<String> names = this.labels.stream().map(l->l.name).collect(Collectors.toSet());
        return !group.labels.stream().anyMatch(l->names.contains(l.name));
    }
    public List<Label> getConflicts(LabelGroup group){
        Set<String> names = this.labels.stream().map(l->l.name).collect(Collectors.toSet());
        return group.labels.stream().filter(l->names.contains(l.name)).toList();
    }
    public boolean loadGroup(LabelGroup group){
        Map<String,Label> sources = new HashMap<>();
        labels.forEach(l->{
            sources.put(l.name,l);
            sources.put(l.getFqdn(),l);
        });
        if(!canLoad(group)){
            return false;
        }else{
            group.labels.forEach(l->{
                Label copy = l.copy(sources::get);

            });
        }
        return true;
    }
    public boolean hasTempLabel(Extractor e){
        return Extractor.Type.VALUE.equals(e.type)
            && (
                (
                    !e.isPersistent() && !e.targetLabel.isPersistent()
                ) || (
                    !e.targetLabel.group.equals(this)
                )
            );
    }
    @PreUpdate
    @PrePersist
    public void checkLabels(){
        boolean needChecking = labels != null && labels.stream().flatMap(l -> l.extractors.stream()).anyMatch(this::hasTempLabel);
        if(needChecking){
            Map<String,Label> sources = new HashMap<>();
            labels.stream().forEach(l->{
                sources.put(l.name,l);
                sources.put(l.getFqdn(),l);
            });
            labels.stream().flatMap(l->l.extractors.stream())
                    .filter(this::hasTempLabel)
                    .forEach(e->{
                        String targetName = e.targetLabel.name;
                        if(sources.containsKey(targetName)){
                            e.targetLabel = sources.get(targetName);
                        }

                    });
        }
        //Collections.sort(labels);
        this.labels = Label.kahnDagSort(this.labels);
    }

    public boolean sameOwner(String owner){
        return owner !=null && owner.equals(this.owner);
    }

    @Override
    public int compareTo(LabelGroup o) {
        return 0;
    }
}
