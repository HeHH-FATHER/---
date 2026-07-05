package com.neu.clean;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 深渊数据清洗常量定义
 * 对照课件 4.数据清洗.md — Constants.java
 */
public final class AbyssCleanConstants {

    private AbyssCleanConstants() {}

    // ==================== 合法星级 ====================
    public static final Set<Integer> VALID_STARS = new HashSet<>(Arrays.asList(4, 5));

    // ==================== 合法梯度 ====================
    public static final Set<String> VALID_TIERS = new HashSet<>(Arrays.asList(
        "s1", "s", "a", "b", "c", "f", ""
    ));

    // ==================== 数值范围 ====================
    public static final double MIN_RATE = 0.0;
    public static final double MAX_RATE = 100.0;
    public static final int MIN_COUNT = 0;

    // ==================== Mapper Key 前缀 ====================
    // C 前缀: 干净数据，Reducer直接透传到 dwd/
    // X 前缀: 脏数据，Reducer路由到 dirty/ 对应子目录
    public static final String PREFIX_CLEAN = "C";   // C|行数据
    public static final String PREFIX_DIRTY = "X";   // X|{脏数据类型}|行数据

    // ==================== 脏数据类型 ====================
    public static final String DIRTY_MISSING_FIELD = "missing_field";
    public static final String DIRTY_BAD_STAR      = "bad_star";
    public static final String DIRTY_BAD_RATE      = "bad_rate";
    public static final String DIRTY_BAD_COUNT     = "bad_count";
    public static final String DIRTY_BAD_TIER      = "bad_tier";
    public static final String DIRTY_PARSE_ERROR   = "parse_error";
    public static final String DIRTY_DUPLICATE     = "duplicate";

    // ==================== 计数器 ====================
    public enum Counter {
        TOTAL_RECORDS,
        CLEAN_PASSED,
        DIRTY_MISSING,
        DIRTY_BAD_STAR,
        DIRTY_BAD_RATE,
        DIRTY_BAD_COUNT,
        DIRTY_BAD_TIER,
        DIRTY_PARSE,
        DUPLICATE_REMOVED
    }
}
