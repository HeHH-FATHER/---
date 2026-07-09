package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Weapon usage statistic from aggregate input (角色练度统计.json).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeaponStat {
    /** Weapon name, e.g. "焚曜千阳" */
    private String name;
    /** Avatar icon URL (not used in generation) */
    private String avatar;
    /** Usage rate as a percentage (0–100), e.g. 51.9 means 51.9% of players use this weapon */
    private double rate;

    public WeaponStat() {}

    public WeaponStat(String name, double rate) {
        this.name = name;
        this.rate = rate;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getAvatar() { return avatar; }
    public void setAvatar(String avatar) { this.avatar = avatar; }

    public double getRate() { return rate; }
    public void setRate(double rate) { this.rate = rate; }
}
