package io.hyperfoil.tools.exp.horreum.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Function that runs against the values extracted for the label (currently javascript)
 */
@Entity
@Table(name = "label_reducers")
public class LabelReducer {
    @Id
    public Integer id;
    public String function;
}
