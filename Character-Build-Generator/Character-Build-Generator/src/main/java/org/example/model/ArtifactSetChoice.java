package org.example.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A single artifact set choice in an output build record.
 */
@JsonPropertyOrder({"name"})
public class ArtifactSetChoice implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    /** Artifact set name, e.g. "黑曜秘典4" */
    private String name;

    public ArtifactSetChoice() {}

    public ArtifactSetChoice(String name) {
        this.name = name;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
