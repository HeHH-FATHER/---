package com.neu.abyss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * ═══════════════════════════════════════════════════════════════
 * 逐风数据洞察平台 — DWS 层 聚合 MR
 * ═══════════════════════════════════════════════════════════════
 *
 * 层级流: DWD(HDFS CSV) → DWS(HDFS CSV)
 * 下游:   ADS 层 SQL 脚本 → MySQL
 *
 * 输入: /data/abyss/dwd/dwd_char_detail/r-*
 *       CSV 格式: version,uid,char_name,star,constellation,level,used_in_abyss
 *
 * 聚合维度: 按 (version, char_name) 聚合
 *
 * 输出: /data/abyss/dws/dws_char_summary/
 *       CSV 格式: version,char_name,star,own_count,use_count,total_users,
 *                 own_rate,use_rate,avg_constellation,avg_level
 *
 * 提交命令:
 *   hadoop jar abyss-mr.jar com.neu.abyss.AbyssAggMR \
 *     /data/abyss/dwd/dwd_char_detail/ \
 *     /data/abyss/dws/dws_char_summary/
 *
 * @author 逐风数据洞察平台
 * @version 5.0
 */
public class AbyssAggMR extends Configured implements Tool {

    // ═══════════════════════════════════════════
    // 计数器
    // ═══════════════════════════════════════════
    private enum Counter {
        INPUT_ROWS,
        OUTPUT_GROUPS,
        TOTAL_USERS_DISTINCT
    }

    // ═══════════════════════════════════════════
    // Mapper
    // ═══════════════════════════════════════════

    /**
     * 输入: dwd_char_detail 行
     *   version,uid,char_name,star,constellation,level,used_in_abyss
     * 输出:
     *   Key:   version|char_name
     *   Value: star|constellation|level|used_in_abyss
     *
     * 注意: total_users 需要按 version 全局计数。这里我们先将每行
     * 的 uid 信息也传下去，让 Reducer 通过额外逻辑计算。
     * 但标准 MR 中 Reducer 只看到按 char_name 聚合的数据。
     *
     * 解决办法: 额外发送一个特殊的 TOTAL|{version}|{uid} 记录，
     *           让 Reducer 知道有哪些用户。
     *           或者: 在 Driver 中先扫描一次获得 total_users。
     *
     * 我们采用方案2: Driver 中先用 wc 思路预计算每个 version 的用户数，
     * 写入 Configuration，Reducer 读取。
     *
     * 简化方案: 因为 char_detail 中每个用户对每个角色都有一行，
     * 一个 version 的总用户数 = 该 version 下任意一个热门角色的 own_count。
     * 这不可靠。
     *
     * 最佳简化: 在 Mapper 中额外发送全局计数标记行:
     *   Key:   {version}|__TOTAL__
     *   Value: {uid}
     * Reducer 在 __TOTAL__ 组中去重 uid → 得到 total_users_per_version。
     *
     * 但 Reducer 是分区-排序的，不同 key 在不同 Reducer...
     * 使用自定义 Partitioner 让同一 version 的数据去同一个 Reducer。
     */
    public static class AggMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] f = line.split(",", -1);
            // 期望 7 列: version,uid,char_name,star,constellation,level,used
            if (f.length < 7) return;

            String version     = f[0].trim();
            String uid         = f[1].trim();
            String charName    = f[2].trim();
            String star        = f[3].trim();
            String cons        = f[4].trim();
            String level       = f[5].trim();
            String usedInAbyss = f[6].trim();

            if (version.isEmpty() || charName.isEmpty()) return;

            context.getCounter(Counter.INPUT_ROWS).increment(1);

            // ── 角色聚合键 ──
            outKey.set(version + "|" + charName);
            // Value: star|constellation|level|used_in_abyss|uid
            outVal.set(String.format("%s|%s|%s|%s|%s", star, cons, level, usedInAbyss, uid));
            context.write(outKey, outVal);

            // ── 全局用户计数键（用于去重计算 total_users）──
            outKey.set(version + "|__UID__");
            outVal.set(uid);
            context.write(outKey, outVal);
        }
    }

    // ═══════════════════════════════════════════
    // Partitioner: 同一 version → 同一 Reducer
    // ═══════════════════════════════════════════

    public static class VersionPartitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // key = version|... → 取 version 部分做 hash
            String ks = key.toString();
            int pipe = ks.indexOf('|');
            String version = (pipe > 0) ? ks.substring(0, pipe) : ks;
            return Math.abs(version.hashCode()) % numPartitions;
        }
    }

    // ═══════════════════════════════════════════
    // GroupingComparator: version|charName 和 version|__UID__ 不进同一组
    // 默认按整个 key 分组即可（因为 __UID__ ≠ 角色名）
    // ═══════════════════════════════════════════

    // ═══════════════════════════════════════════
    // Reducer
    // ═══════════════════════════════════════════

    public static class AggReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        // 当前 version 的去重 uid 集合（用于计算 total_users）
        private String currentVersion = null;
        private final Set<String> uidSet = new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String ks = key.toString();
            int pipe = ks.indexOf('|');
            String version  = ks.substring(0, pipe);
            String charName = ks.substring(pipe + 1);

            // ── __UID__ 特殊键：收集去重用户 ──
            if ("__UID__".equals(charName)) {
                // 版本切换时输出上一个版本的 total_users
                if (currentVersion != null && !currentVersion.equals(version)) {
                    uidSet.clear();
                }
                currentVersion = version;
                for (Text v : values) {
                    uidSet.add(v.toString());
                }
                context.getCounter(Counter.TOTAL_USERS_DISTINCT).increment(1);
                return;
            }

            // ── 正常角色聚合 ──
            int totalUsers = uidSet.size();  // 当前版本的去重用户数
            if (totalUsers == 0) totalUsers = 1;  // 防止除零

            int star = 0;
            long ownCount = 0;
            long useCount = 0;
            double sumConstellation = 0;
            double sumLevel = 0;

            for (Text v : values) {
                // Value: star|constellation|level|used_in_abyss|uid
                String[] parts = v.toString().split("\\|", -1);
                if (parts.length < 4) continue;

                star = Integer.parseInt(parts[0]);
                double cons  = Double.parseDouble(parts[1]);
                double lvl   = Double.parseDouble(parts[2]);
                int used     = Integer.parseInt(parts[3]);

                ownCount++;
                sumConstellation += cons;
                sumLevel += lvl;
                if (used == 1) {
                    useCount++;
                }
            }

            double ownRate = (double) ownCount / totalUsers * 100.0;
            double useRate = (double) useCount / totalUsers * 100.0;
            double avgConstellation = sumConstellation / ownCount;
            double avgLevel = sumLevel / ownCount;

            // ── 输出聚合行 ──
            // CSV: version,char_name,star,own_count,use_count,total_users,
            //       own_rate,use_rate,avg_constellation,avg_level
            outKey.set(version + "," + charName);
            outVal.set(String.format("%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f",
                    star, ownCount, useCount, totalUsers,
                    ownRate, useRate, avgConstellation, avgLevel));
            context.write(outKey, outVal);

            context.getCounter(Counter.OUTPUT_GROUPS).increment(1);
        }
    }

    // ═══════════════════════════════════════════
    // Driver
    // ═══════════════════════════════════════════

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("══════════════════════════════════════════");
            System.err.println("AbyssAggMR — DWS 层 聚合");
            System.err.println("══════════════════════════════════════════");
            System.err.println("用法: hadoop jar abyss-mr.jar com.neu.abyss.AbyssAggMR <输入> <输出>");
            System.err.println("  <输入>  DWD char_detail 路径, 如 /data/abyss/dwd/dwd_char_detail/");
            System.err.println("  <输出>  DWS 输出路径,     如 /data/abyss/dws/dws_char_summary/");
            System.err.println("");
            System.err.println("产出: /data/abyss/dws/dws_char_summary/part-r-*");
            System.err.println("  CSV: version,char_name,star,own_count,use_count,");
            System.err.println("       total_users,own_rate,use_rate,avg_constellation,avg_level");
            System.err.println("══════════════════════════════════════════");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AbyssAggMR-DWS");

        job.setJarByClass(AbyssAggMR.class);
        job.setMapperClass(AggMapper.class);
        job.setReducerClass(AggReducer.class);
        job.setPartitionerClass(VersionPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 按同一版本聚合 → 多少个版本就用多少个 Reducer
        // 默认用 1 个（因为我们需要 total_users 跨角色共享）
        // 如果多版本，可增加 Reducer 数，但需注意 __UID__ 去重逻辑
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 删除已存在的输出目录
        org.apache.hadoop.fs.FileSystem fs =
                org.apache.hadoop.fs.FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean success = job.waitForCompletion(true);

        if (success) {
            long inRows = job.getCounters().findCounter(Counter.INPUT_ROWS).getValue();
            long outGroups = job.getCounters().findCounter(Counter.OUTPUT_GROUPS).getValue();
            System.out.println("══════════════════════════════════════════");
            System.out.println("DWS 聚合完成!");
            System.out.println("  输入行数:     " + inRows);
            System.out.println("  输出角色组:   " + outGroups);
            System.out.println("══════════════════════════════════════════");
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AbyssAggMR(), args);
        System.exit(exitCode);
    }
}
