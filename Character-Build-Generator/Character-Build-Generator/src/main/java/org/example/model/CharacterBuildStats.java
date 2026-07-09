package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * Aggregate build statistics for a single character (input format from 角色练度统计.json).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CharacterBuildStats {
    /** Character name in Chinese, e.g. "玛薇卡" */
    private String role;
    /** Character name in English, e.g. "Mavuika" */
    private String ename;
    /** Rarity stars (4 or 5) */
    private int star;
    /** Number of players who own this character */
    private int player_count;
    /** Average character level across all owners */
    private double avg_level;
    /** Average constellation level (0–6) */
    private double avg_constellation;
    /** Average Normal Attack talent level */
    private double talent_na;
    /** Average Elemental Skill talent level */
    private double talent_skill;
    /** Average Elemental Burst talent level */
    private double talent_burst;
    /** Constellation distribution: keys "c0"–"c6", values are percentages (0–100) */
    private Map<String, Double> constellation_dist;
    /** Average damage dealt (reported value) */
    private int avg_damage;
    /** Type of damage being measured, e.g. "Q爆发伤害" */
    private String damage_type;
    /** Top-N most used weapons with usage rates */
    private List<WeaponStat> weapons;
    /** Top-N most used artifact sets with usage rates */
    private List<ArtifactSetStat> artifact_sets;

    public CharacterBuildStats() {}

    // ── getters / setters ────────────────────────────────────────────────

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public String getEname() { return ename; }
    public void setEname(String ename) { this.ename = ename; }

    public int getStar() { return star; }
    public void setStar(int star) { this.star = star; }

    public int getPlayer_count() { return player_count; }
    public void setPlayer_count(int player_count) { this.player_count = player_count; }

    public double getAvg_level() { return avg_level; }
    public void setAvg_level(double avg_level) { this.avg_level = avg_level; }

    public double getAvg_constellation() { return avg_constellation; }
    public void setAvg_constellation(double avg_constellation) { this.avg_constellation = avg_constellation; }

    public double getTalent_na() { return talent_na; }
    public void setTalent_na(double talent_na) { this.talent_na = talent_na; }

    public double getTalent_skill() { return talent_skill; }
    public void setTalent_skill(double talent_skill) { this.talent_skill = talent_skill; }

    public double getTalent_burst() { return talent_burst; }
    public void setTalent_burst(double talent_burst) { this.talent_burst = talent_burst; }

    public Map<String, Double> getConstellation_dist() { return constellation_dist; }
    public void setConstellation_dist(Map<String, Double> constellation_dist) {
        this.constellation_dist = constellation_dist;
    }

    public int getAvg_damage() { return avg_damage; }
    public void setAvg_damage(int avg_damage) { this.avg_damage = avg_damage; }

    public String getDamage_type() { return damage_type; }
    public void setDamage_type(String damage_type) { this.damage_type = damage_type; }

    public List<WeaponStat> getWeapons() { return weapons; }
    public void setWeapons(List<WeaponStat> weapons) { this.weapons = weapons; }

    public List<ArtifactSetStat> getArtifact_sets() { return artifact_sets; }
    public void setArtifact_sets(List<ArtifactSetStat> artifact_sets) { this.artifact_sets = artifact_sets; }
}
