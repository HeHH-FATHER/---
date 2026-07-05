package com.neu.clean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 深渊数据清洗 Mapper
 * 对照课件 4.数据清洗.md — DataCleanMapper.java
 *
 * Key 设计（对照课件 2.4 节）:
 *   C|{行数据}         →  干净数据，Reducer 透传到 dwd/
 *   X|{脏数据类型}|{行数据} →  脏数据，Reducer 路由到 dirty/
 *
 * 清洗规则（对照课件 2.2 十条清洗规则）:
 *   1. 必须字段缺失（name为空）  → dirty/missing_field
 *   2. 星级不在 4/5 范围内       → dirty/bad_star
 *   3. 使用率超出 0-100          → dirty/bad_rate
 *   4. 持有率超出 0-100          → dirty/bad_rate
 *   5. 使用次数为负数            → dirty/bad_count
 *   6. 梯度不在合法集合内        → dirty/bad_tier
 *   7. 解析失败                  → dirty/parse_error
 *   8. 其余 → 干净数据透传
 *
 * CSV 格式: version, name, star, use, own, use_rate, own_rate, tier
 */
public class AbyssCleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 复用对象
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        context.getCounter(AbyssCleanConstants.Counter.TOTAL_RECORDS).increment(1);

        // 跳过表头
        if (line.startsWith("version") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",", -1);  // -1 保留尾部空字段
        if (fields.length < 8) {
            routeDirty(context, AbyssCleanConstants.DIRTY_PARSE_ERROR, line);
            return;
        }

        String version   = fields[0].trim();
        String charName  = fields[1].trim();
        String starStr   = fields[2].trim();
        String useStr    = fields[3].trim();
        String ownStr    = fields[4].trim();
        String useRateStr = fields[5].trim();
        String ownRateStr = fields[6].trim();
        String tier      = fields[7].trim();

        // ===== 规则1: 必须字段缺失 =====
        if (charName.isEmpty() || version.isEmpty()) {
            routeDirty(context, AbyssCleanConstants.DIRTY_MISSING_FIELD, line);
            return;
        }

        // ===== 规则2: 星级校验 =====
        int star;
        try {
            star = Integer.parseInt(starStr);
        } catch (NumberFormatException e) {
            routeDirty(context, AbyssCleanConstants.DIRTY_PARSE_ERROR, line);
            return;
        }
        if (!AbyssCleanConstants.VALID_STARS.contains(star)) {
            routeDirty(context, AbyssCleanConstants.DIRTY_BAD_STAR, line);
            return;
        }

        // ===== 规则3/4: 使用率/持有率范围 =====
        try {
            double useRate = Double.parseDouble(useRateStr);
            double ownRate = Double.parseDouble(ownRateStr);
            if (useRate < AbyssCleanConstants.MIN_RATE || useRate > AbyssCleanConstants.MAX_RATE
                || ownRate < AbyssCleanConstants.MIN_RATE || ownRate > AbyssCleanConstants.MAX_RATE) {
                routeDirty(context, AbyssCleanConstants.DIRTY_BAD_RATE, line);
                return;
            }
        } catch (NumberFormatException e) {
            routeDirty(context, AbyssCleanConstants.DIRTY_PARSE_ERROR, line);
            return;
        }

        // ===== 规则5: 使用次数校验 =====
        try {
            int useCount = Integer.parseInt(useStr);
            int ownCount = Integer.parseInt(ownStr);
            if (useCount < AbyssCleanConstants.MIN_COUNT || ownCount < AbyssCleanConstants.MIN_COUNT) {
                routeDirty(context, AbyssCleanConstants.DIRTY_BAD_COUNT, line);
                return;
            }
        } catch (NumberFormatException e) {
            routeDirty(context, AbyssCleanConstants.DIRTY_PARSE_ERROR, line);
            return;
        }

        // ===== 规则6: 梯度校验 =====
        if (!AbyssCleanConstants.VALID_TIERS.contains(tier)) {
            routeDirty(context, AbyssCleanConstants.DIRTY_BAD_TIER, line);
            return;
        }

        // ===== 通过全部校验 → 干净数据 =====
        // 按 C|offset 作为 Key（确保唯一,不用去重）
        String dedupKey = charName + "|" + version;
        outputKey.set(AbyssCleanConstants.PREFIX_CLEAN + "|" + dedupKey);
        outputValue.set(line);
        context.write(outputKey, outputValue);
        context.getCounter(AbyssCleanConstants.Counter.CLEAN_PASSED).increment(1);
    }

    /**
     * 脏数据路由 — 对照课件 Mapper 中 X 前缀设计
     */
    private void routeDirty(Context context, String dirtyType, String line)
            throws IOException, InterruptedException {

        outputKey.set(AbyssCleanConstants.PREFIX_DIRTY + "|" + dirtyType + "|"
                      + System.currentTimeMillis() + "_" + Math.random());
        outputValue.set(line);
        context.write(outputKey, outputValue);

        // 计数器
        switch (dirtyType) {
            case AbyssCleanConstants.DIRTY_MISSING_FIELD:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_MISSING).increment(1); break;
            case AbyssCleanConstants.DIRTY_BAD_STAR:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_BAD_STAR).increment(1); break;
            case AbyssCleanConstants.DIRTY_BAD_RATE:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_BAD_RATE).increment(1); break;
            case AbyssCleanConstants.DIRTY_BAD_COUNT:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_BAD_COUNT).increment(1); break;
            case AbyssCleanConstants.DIRTY_BAD_TIER:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_BAD_TIER).increment(1); break;
            default:
                context.getCounter(AbyssCleanConstants.Counter.DIRTY_PARSE).increment(1); break;
        }
    }
}
