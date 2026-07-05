package org.example.util;

import java.util.Random;

/**
 * 可复现的随机数生成器。
 *
 * 包装 Java 标准 Random，记录 seed 以便：
 * 1. 相同 seed → 相同输出序列（调试 / 复验）
 * 2. 每个用户通过 "seed + userIdx * 大质数" 得到独立子序列
 */
public class SeededRandom {

    /** Java 标准随机数引擎 */
    private final Random rng;

    /** 初始化种子（可通过 getSeed() 查看） */
    private final long seed;

    /**
     * @param seed 随机种子，相同种子产生相同序列
     */
    public SeededRandom(long seed) {
        this.seed = seed;
        this.rng = new Random(seed);
    }

    /** 返回此实例的种子值 */
    public long getSeed() {
        return seed;
    }

    /**
     * @return [0, 1) 之间的均匀分布 double
     */
    public double nextDouble() {
        return rng.nextDouble();
    }

    /**
     * @param bound 上界（不含）
     * @return [0, bound) 之间的均匀分布 int
     */
    public int nextInt(int bound) {
        return rng.nextInt(bound);
    }

    /**
     * @param max 上界（不含）
     * @return [0, max) 之间的均匀分布 double
     */
    public double nextDouble(double max) {
        return rng.nextDouble() * max;
    }
}
