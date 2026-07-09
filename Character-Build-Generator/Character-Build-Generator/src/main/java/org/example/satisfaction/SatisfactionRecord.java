package org.example.satisfaction;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 满意度记录 — 写入 Kafka / JSON 的消息格式
 * 与 Python satisfaction_producer.py 输出的 Redis 字段一致
 */
public class SatisfactionRecord implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    @JsonProperty("role")
    private String role;

    @JsonProperty("star")
    private int star;

    @JsonProperty("satify")
    private double satify;

    @JsonProperty("ability")
    private double ability;

    @JsonProperty("look")
    private double look;

    @JsonProperty("vote_sum")
    private long voteSum;

    @JsonProperty("delta")
    private double delta;

    public SatisfactionRecord() {}

    public SatisfactionRecord(String role, int star, double satify, double ability, double look, long voteSum, double delta) {
        this.role = role; this.star = star; this.satify = satify;
        this.ability = ability; this.look = look; this.voteSum = voteSum; this.delta = delta;
    }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public int getStar() { return star; }
    public void setStar(int star) { this.star = star; }
    public double getSatify() { return satify; }
    public void setSatify(double satify) { this.satify = satify; }
    public double getAbility() { return ability; }
    public void setAbility(double ability) { this.ability = ability; }
    public double getLook() { return look; }
    public void setLook(double look) { this.look = look; }
    public long getVoteSum() { return voteSum; }
    public void setVoteSum(long voteSum) { this.voteSum = voteSum; }
    public double getDelta() { return delta; }
    public void setDelta(double delta) { this.delta = delta; }
}
