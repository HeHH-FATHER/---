package org.example.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A single weapon choice in an output build record.
 */
@JsonPropertyOrder({"name", "refinement"})
public class WeaponChoice implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    /** Weapon name */
    private String name;
    /** Refinement rank (1–5); 1 = R1, 5 = R5 */
    private int refinement;

    public WeaponChoice() {}

    public WeaponChoice(String name, int refinement) {
        this.name = name;
        this.refinement = refinement;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getRefinement() { return refinement; }
    public void setRefinement(int refinement) { this.refinement = refinement; }
}
