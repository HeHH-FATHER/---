package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Artifact set usage statistic from aggregate input (角色练度统计.json).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ArtifactSetStat {
    /** Artifact set name, e.g. "黑曜秘典4" */
    private String name;
    /** Avatar icon URLs for set pieces (not used in generation) */
    private List<String> avatars;
    /** Usage rate as a percentage (0–100) */
    private double rate;

    public ArtifactSetStat() {}

    public ArtifactSetStat(String name, double rate) {
        this.name = name;
        this.rate = rate;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<String> getAvatars() { return avatars; }
    public void setAvatars(List<String> avatars) { this.avatars = avatars; }

    public double getRate() { return rate; }
    public void setRate(double rate) { this.rate = rate; }
}
