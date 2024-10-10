package io.hyperfoil.tools.exp.horreum.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.valid.ValidTarget;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Entity
@Table(
        name = "label",
        indexes = {
                @Index(name = "label_targetschema", columnList = "target_schema", unique = false),
                @Index(name = "label_parent", columnList = "group_id", unique = false)
        }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Label extends PanacheEntity implements Comparable<Label> {

    public static enum MultiIterationType { Length, NxN}
    public static enum ScalarVariableMethod { First, All}

    public static Function<String,Label> DEFAULT_RESOLVER = str->Label.find("name",str).firstResult();

    public static Label findReference(String name,Long testId){
        if (!name.contains(Extractor.NAME_SEPARATOR)) {
            return Label.find("from Label l where l.name=?1 and l.parent.id=?2",name,testId).firstResult();
        }else{
            String split[] = name.split(":");
            if(split.length==2){//includes parent or schema
                //lookup the split[0] to see if it matches a schema name? (check all labels for target_schema

            }else if (split.length==3){//includes parent and schema

            }
            return Label.find("name",name).firstResult();
        }
    }
    //based on https://github.com/williamfiset/Algorithms/blob/master/src/main/java/com/williamfiset/algorithms/graphtheory/Kahns.java
    public static List<Label> kahnDagSort(List<Label> labels){
        Map<String, AtomicInteger> inDegrees = new HashMap<>();
        if(labels == null || labels.isEmpty()){
            return labels;
        }
        labels.forEach(l->{
            inDegrees.put(l.name,new AtomicInteger(0));
        });
        labels.forEach(l->{
            l.extractors.stream()
                    .filter(e->Extractor.Type.VALUE.equals(e.type))
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
                    .filter(e->Extractor.Type.VALUE.equals(e.type))
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
            labels.forEach(l->{
                if(inDegrees.get(l.name).get() > 0){
                    rtrn.add(0,l);//they will then go to the back
                }
            });
        }
        //reverse because of graph direction
        Collections.reverse(rtrn);
        return rtrn;
    }



    @Pattern(regexp = "^[^{].*[^}]$",message = "Label names cannot start with '{' or end with '}'")
    @Pattern(regexp = "^[^$].+",message = "Label name cannot start with '$'")
    @Pattern(regexp = ".*(?<!\\[])$",message = "Label name cannot end with '[]'")
    public String name;

    //a label in a labelGroup would have a null test, how do we conditionally validate that?
    @NotNull(message = "label must reference a group")
    @ManyToOne(cascade = {CascadeType.ALL})
    @JoinColumn(name = "group_id")
    @JsonIgnore
    public LabelGroup group; //using string to simplify the PoC

    @ManyToOne
    public Label sourceLabel; //the label that substitutes for the Run from the perspectice of this run

    @ManyToOne
    public LabelGroup sourceGroup; //

    @ManyToOne(cascade = {CascadeType.ALL})
    public LabelGroup targetGroup;

    @ElementCollection(fetch = FetchType.EAGER)
    @OneToMany(cascade = {CascadeType.PERSIST,CascadeType.MERGE}, fetch = FetchType.EAGER, orphanRemoval = true, mappedBy = "parent")
    public List<@NotNull(message="null extractors are not supported") @ValidTarget Extractor> extractors;

    @ManyToOne(cascade = {CascadeType.PERSIST,CascadeType.MERGE})
    public LabelReducer reducer;

    @Enumerated(EnumType.STRING)
    public MultiIterationType multiType = MultiIterationType.Length;

    @Enumerated(EnumType.STRING)
    public ScalarVariableMethod scalarMethod = ScalarVariableMethod.First;

    public Label(){}
    public Label(String name){
        this(name,null);
    }
    public Label(String name,LabelGroup parent){
        this.name = name;
        this.group = parent;
        this.extractors = new ArrayList<>();
    }

    public Label loadExtractors(List<Extractor> extractors){
        this.extractors = extractors;
        this.extractors.forEach(e->e.parent=this);
        return this;
    }
    public Label loadExtractors(Extractor...extractors){
        this.extractors = Arrays.asList(extractors);
        this.extractors.forEach(e->e.parent=this);
        return this;
    }
    public Label setTargetSchema(LabelGroup targetSchema){
        this.targetGroup = targetSchema;
        return this;
    }

    public boolean hasReducer(){return reducer!=null;}
    public Label setReducer(String javascript){
        return setReducer(new LabelReducer(javascript));
    }
    public Label setReducer(LabelReducer reducer){
        this.reducer = reducer;
        return this;
    }

    public Label setGroup(LabelGroup group){
        this.group = group;
        return this;
    }
    public boolean hasSourceLabel(){return sourceLabel !=null;}
    public boolean hasSourceGroup(){return sourceGroup!=null;}
    public LabelGroup getGroup(){return group;}


    @Override
    public String toString(){return "label=[name:"+name+" id:"+id+" group:"+(group==null?"null":group.id)+" extractors="+(extractors==null?"null":extractors.stream().map(e->e.name).collect(Collectors.toList()))+"]";}


    public String getFqdn(){
        return (sourceLabel !=null ? sourceLabel.name+":" : "") + (group!=null ? group.name+":" : "") + name;
    }

    public Label copy(Function<String,Label> resolver){
        Label newLabel = new Label();

        newLabel.name = this.name;
        newLabel.group = this.group;
        newLabel.multiType = this.multiType;
        newLabel.scalarMethod = this.scalarMethod;
        if(hasReducer()) {
            newLabel.setReducer(this.reducer.function);
        }
        if( this.hasSourceLabel() ){
            newLabel.sourceLabel = resolver.apply(this.sourceLabel.getFqdn());
        }
        List<Extractor> newExtractors = new ArrayList<>();
        for(Extractor e : extractors){
            newExtractors.add(e.copy(resolver));
        }
        newLabel.loadExtractors(newExtractors);
        return newLabel;
    }
    public void unloadGroup(LabelGroup group){
        Label.find("from Label l where l.group=?1 and l.groupSource=?2 and l.group=?3",this.group,this,group)
                .stream().forEach(found->{
                    this.group.labels.remove(found);
                });
    }
    public void loadGroup(LabelGroup group){
        Map<String,Label> scope = new HashMap<>();
        scope.put(this.getFqdn(),this);
        scope.put(this.name,this);
        //they need to be sorted to ensure extractor dependencies are available
        List<Label> sorted = Label.kahnDagSort(group.labels);
        for(Label groupLabel : sorted){
            Label copy = groupLabel.copy(scope::get);
            scope.put(copy.getFqdn(),copy);
            scope.put(copy.name,copy);
            copy.sourceLabel =this;
            copy.extractors.forEach(extractor -> {
                if(Extractor.Type.PATH.equals(extractor.type)){
                    extractor.type = Extractor.Type.VALUE;
                    extractor.targetLabel = this;
                }
                //VALUE extractor targetLabel were handled by Extractor.copy
                //what to do about METADATA?
            });
            if(copy.group!=null){
                copy.sourceGroup = copy.group;
            }
            if(this.group!=null){
                copy.group=this.group;
                this.group.labels.add(copy);
            }
        }
    }

    @Override
    public boolean equals(Object o){
        if(!(o instanceof Label)){
            return false;
        }
        Label o1 = (Label)o;
        boolean rtrn = Objects.equals(this.id, o1.id) && this.name.equals(o1.name) && Objects.equals(this.group,o1.group);
        return rtrn;
    }
    @Override
    public int compareTo(Label o1) {
        int rtrn = 0;
        if(o1.usesLabelValueExtractor() && !this.usesLabelValueExtractor()){
            rtrn = -1;//o1 is less than 02
        }else if (this.usesLabelValueExtractor() && !o1.usesLabelValueExtractor()){
            rtrn = 1;//o2 is less than o1
        }else if (o1.dependsOn(this)){
            rtrn = -1;//o1 has to come after o2
        }else if (this.dependsOn(o1)) {
            rtrn = 1;//o1 must come before o2
        }else if (this.labelValueExtractorCount() > o1.labelValueExtractorCount()) {
            rtrn = 1;
        }else if ( o1.labelValueExtractorCount() > this.labelValueExtractorCount()){
            rtrn = -1;
        }else{
            //unable to compare them, assume "equal" rank?
        }
        return rtrn;
    }


    public long labelValueExtractorCount(){
        return extractors.stream().filter(e-> Extractor.Type.VALUE.equals(e.type)).count();
    }
    public long forEachCount(){
        return extractors.stream().filter(e->e.forEach).count();
    }
    public boolean hasForEach(){
        return extractors.stream().anyMatch(e->e.forEach);
    }
    public boolean usesOnlyJsonpathExtractor(){
        return extractors.stream().allMatch(e->Extractor.Type.PATH.equals(e.type));
    }
    public boolean usesLabelValueExtractor(){
        return extractors.stream().anyMatch(e->Extractor.Type.VALUE.equals(e.type));
    }
    public boolean dependsOn(Label l){
        //do not replace id == l.id with .equals because id can be null
        return extractors.stream().anyMatch(e->Extractor.Type.VALUE.equals(e.type) && e.targetLabel.id == l.id && (e).targetLabel.name.equals(l.name));
    }

    /**
     * returns true if this is part of a circular reference
     * @return
     */
    @JsonIgnore
    public boolean isCircular(){
        if(!usesLabelValueExtractor()){
            return false;
        }
        Queue<Label> todo = new PriorityQueue<>();
        extractors.stream()
                .filter(e->Extractor.Type.VALUE.equals(e.type))
                .map(e->e.targetLabel).forEach(todo::add);
        Label target;
        boolean ok = true;
        while( ok && (target = todo.poll()) !=null ){
            if(this.equals(target)){
                ok = false;
            }
            if(target.extractors!=null) {
                List<Label> targetLabels = target.extractors.stream()
                        .filter(e->Extractor.Type.VALUE.equals(e.type))
                        .map(e->e.targetLabel)
                        .toList();
                todo.addAll(targetLabels);
            }else{
                //extractors can be null for auto-created labels inside LabelValueExtactor :(
            }
        }
        return !ok;
    }
}
