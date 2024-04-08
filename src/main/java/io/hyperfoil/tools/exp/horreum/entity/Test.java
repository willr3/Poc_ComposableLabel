package io.hyperfoil.tools.exp.horreum.entity;

import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import io.hyperfoil.tools.exp.horreum.valid.ValidLabel;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.Valid;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Entity
public class Test extends PanacheEntity {

    public String name;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "test_labels")
    @OneToMany(cascade = {CascadeType.PERSIST,CascadeType.MERGE}, fetch = FetchType.EAGER, orphanRemoval = true, mappedBy = "parent")
    //Tried ManyToMany but it resulted in Labels not cascading persist
    public List<@ValidLabel Label> labels;

    public Test(){}
    public Test(String name){
        this.name = name;
    }
    public Test loadLabels(@Valid Label...definitions){
        this.labels = new ArrayList(Arrays.asList(definitions));
        checkLabels();
        return this;
    }
    public boolean hasTempLabel(Extractor e){
        return e instanceof LabelValueExtractor
        && (
            (
                !e.isPersistent()
                && !((LabelValueExtractor)e).targetLabel.isPersistent()
            )
            || (
                !((LabelValueExtractor) e).targetLabel.parent.equals(this)
            )
        );
    }
    //based on https://github.com/williamfiset/Algorithms/blob/master/src/main/java/com/williamfiset/algorithms/graphtheory/Kahns.java
    public void kahnDagSort(){
        Map<String, AtomicInteger> inDegrees = new HashMap<>();
        if(labels == null || labels.isEmpty()){
            return;
        }
        labels.forEach(l->{
            inDegrees.put(l.name,new AtomicInteger(0));
        });
        labels.forEach(l->{
            l.extractors.stream()
                    .filter(e->e instanceof LabelValueExtractor)
                    .map(e->(LabelValueExtractor)e)
                    .forEach(lve->{
                        if(inDegrees.containsKey(lve.targetLabel.name)) {
                            inDegrees.get(lve.targetLabel.name).incrementAndGet();
                        }
                    });
        });
        Queue<Label> q = new ArrayDeque<>();
        labels.forEach(l->{
            if(inDegrees.get(l.name).get()==0){
                q.offer(l);
            }
        });
        List<Label> rtrn = new ArrayList<>();
        while(!q.isEmpty()){
            Label l = q.poll();
            rtrn.add(l);
            l.extractors.stream()
                    .filter(e->e instanceof LabelValueExtractor)
                    .map(e->(LabelValueExtractor)e)
                    .forEach(lve->{
                        if(inDegrees.containsKey(lve.targetLabel.name)) {
                            int newDegree = inDegrees.get(lve.targetLabel.name).decrementAndGet();
                            if (newDegree == 0) {
                                q.offer(lve.targetLabel);
                            }
                        }
                    });
        }
        int sum = inDegrees.values().stream().map(a->a.get()).reduce((a,b)->a+b).get();
        if(sum > 0){
            //this means there are loops!!
        }
        //reverse because of graph direction
        Collections.reverse(rtrn);
        labels = rtrn;
    }


    @PreUpdate
    @PrePersist
    public void checkLabels(){
        boolean needChecking = labels != null && labels.stream().flatMap(l -> l.extractors.stream()).anyMatch(this::hasTempLabel);
        if(needChecking){
            Map<String,Label> byName = labels.stream().collect(Collectors.toMap(l->l.name,l->l));
            labels.stream().flatMap(l->l.extractors.stream())
                    .filter(this::hasTempLabel)
                    .map(e-> (LabelValueExtractor)e)
                    .forEach(e->{
                        String targetName = e.targetLabel.name;
                        if(byName.containsKey(targetName)){
                            e.targetLabel = byName.get(targetName);
                        }

                    });
        }
        //Collections.sort(labels);
        kahnDagSort();
    }
}
