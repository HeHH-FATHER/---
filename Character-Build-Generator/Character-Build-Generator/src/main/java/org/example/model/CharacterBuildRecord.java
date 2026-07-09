package org.example.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A single player's character build record (output format).
 */
@JsonPropertyOrder({"role", "ename", "star", "uid", "level", "constellation",
                    "talent_na", "talent_skill", "talent_burst",
                    "avg_damage", "damage_type", "weapon", "artifact_set"})
public class CharacterBuildRecord implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    /** Character name in Chinese */
    private String role;
    /** Character name in English */
    private String ename;
    /** Rarity stars (4 or 5) */
    private int star;
    /** Player UID (9-digit string starting with 180) */
    private String uid;
    /** Character level (1–90) */
    private int level;
    /** Constellation level (0–6) */
    private int constellation;
    /** Normal Attack talent level (1–13) */
    private int talent_na;
    /** Elemental Skill talent level (1–13) */
    private int talent_skill;
    /** Elemental Burst talent level (1–13) */
    private int talent_burst;
    /** Simulated average damage value */
    private int avg_damage;
    /** Type of damage being measured */
    private String damage_type;
    /** Weapon choice with refinement rank */
    private WeaponChoice weapon;
    /** Artifact set choice */
    private ArtifactSetChoice artifact_set;

    public CharacterBuildRecord() {}

    public CharacterBuildRecord(String role, String ename, int star, String uid,
                                int level, int constellation,
                                int talent_na, int talent_skill, int talent_burst,
                                int avg_damage, String damage_type,
                                WeaponChoice weapon, ArtifactSetChoice artifact_set) {
        this.role = role;
        this.ename = ename;
        this.star = star;
        this.uid = uid;
        this.level = level;
        this.constellation = constellation;
        this.talent_na = talent_na;
        this.talent_skill = talent_skill;
        this.talent_burst = talent_burst;
        this.avg_damage = avg_damage;
        this.damage_type = damage_type;
        this.weapon = weapon;
        this.artifact_set = artifact_set;
    }

    // ── getters / setters ────────────────────────────────────────────────

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public String getEname() { return ename; }
    public void setEname(String ename) { this.ename = ename; }

    public int getStar() { return star; }
    public void setStar(int star) { this.star = star; }

    public String getUid() { return uid; }
    public void setUid(String uid) { this.uid = uid; }

    public int getLevel() { return level; }
    public void setLevel(int level) { this.level = level; }

    public int getConstellation() { return constellation; }
    public void setConstellation(int constellation) { this.constellation = constellation; }

    public int getTalent_na() { return talent_na; }
    public void setTalent_na(int talent_na) { this.talent_na = talent_na; }

    public int getTalent_skill() { return talent_skill; }
    public void setTalent_skill(int talent_skill) { this.talent_skill = talent_skill; }

    public int getTalent_burst() { return talent_burst; }
    public void setTalent_burst(int talent_burst) { this.talent_burst = talent_burst; }

    public int getAvg_damage() { return avg_damage; }
    public void setAvg_damage(int avg_damage) { this.avg_damage = avg_damage; }

    public String getDamage_type() { return damage_type; }
    public void setDamage_type(String damage_type) { this.damage_type = damage_type; }

    public WeaponChoice getWeapon() { return weapon; }
    public void setWeapon(WeaponChoice weapon) { this.weapon = weapon; }

    public ArtifactSetChoice getArtifact_set() { return artifact_set; }
    public void setArtifact_set(ArtifactSetChoice artifact_set) { this.artifact_set = artifact_set; }
}
