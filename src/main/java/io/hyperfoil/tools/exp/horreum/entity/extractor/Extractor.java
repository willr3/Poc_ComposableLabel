package io.hyperfoil.tools.exp.horreum.entity.extractor;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all extractors to represent
 */
@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
public abstract class Extractor extends PanacheEntity {

    private static final AtomicInteger counter = new AtomicInteger(0);

    @NotNull(message = "extractor must reference a label")
    @ManyToOne(cascade = CascadeType.PERSIST)
    @JoinColumn(name = "parent_id")
    public Label parent;

    @NotNull(message="extractor name cannot be null")
    public String name;

    public boolean forEach=false;

    public Extractor setName(String name){
        this.name = name;
        return this;
    }

    public static Extractor fromString(String input){
        if(input.startsWith(JsonpathExtractor.PREFIX) || input.startsWith(LabelValueExtractor.FOR_EACH_SUFFIX+LabelValueExtractor.NAME_SEPARATOR)){
            return JsonpathExtractor.fromString(input);
        }else if( input.startsWith(RunMetadataExtractor.PREFIX)){
            return RunMetadataExtractor.fromString(input);
        }else{
            return LabelValueExtractor.fromString(input);
        }
    }

    public static String generateName(){
        return "label_"+String.format("%03d",counter.getAndIncrement());
    }
}
