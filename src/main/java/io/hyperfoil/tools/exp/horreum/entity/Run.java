package io.hyperfoil.tools.exp.horreum.entity;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.exp.horreum.pasted.JsonBinaryType;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import org.hibernate.annotations.Type;

@Entity
public class Run extends PanacheEntity {

    @ManyToOne
    @JoinColumn(name="test_id")
    public Test test;
    @Type(JsonBinaryType.class)
    public JsonNode data;
    @Type(JsonBinaryType.class)
    public JsonNode metadata;

    public Run(){}

    public Run(long testId,JsonNode data,JsonNode metadata){
        this.test = Test.findById(testId);
        this.data = data;
    }
}
