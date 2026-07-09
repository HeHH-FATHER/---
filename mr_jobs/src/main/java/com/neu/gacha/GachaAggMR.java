package com.neu.gacha;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * ═══════════════════════════════════════════════════════════════
 * 逐风数据洞察平台 — DWS 层 抽卡数据聚合 MR
 * ═══════════════════════════════════════════════════════════════
 * 功能: 读取 DWD 层五星抽卡记录，按物品聚合统计 → 写入 MySQL ADS 表。
 *
 * 输入: HDFS JSONL — DWD 输出的 5★ 记录
 * 输出: MySQL ads_gacha_offline — 按物品聚合的五星出货统计
 *
 * 用法:
 *   hadoop jar gacha-mr.jar com.neu.gacha.GachaAggMR /data/gacha/dwd/
 *
 * 调度建议（Crontab）:
 *   每5分钟: hadoop jar gacha-mr.jar com.neu.gacha.GachaAggMR /data/gacha/dwd/
 */
public class GachaAggMR extends Configured implements Tool {

    public static class GachaAggMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final JsonParser parser = new JsonParser();
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            try {
                JsonObject obj = parser.parse(line).getAsJsonObject();
                String item = obj.has("item") ? obj.get("item").getAsString() : "?";
                String type = obj.has("type") ? obj.get("type").getAsString() : "role";
                boolean isUp = obj.has("is_up") && obj.get("is_up").getAsBoolean();

                outKey.set(item);
                outVal.set(type + "," + (isUp ? "1" : "0"));
                context.write(outKey, outVal);
            } catch (Exception e) {
                context.getCounter("Gacha", "MAP_ERR").increment(1);
            }
        }
    }

    public static class GachaAggReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Connection conn;
        private long totalRecords;

        private static final String JDBC_URL = "jdbc:mysql://100.103.177.85:3306/abyss_db"
                + "?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true";
        private static final String JDBC_USER = "root";
        private static final String JDBC_PASS = "123456";

        @Override
        protected void setup(Context context) throws IOException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                conn.setAutoCommit(false);
            } catch (Exception e) {
                throw new IOException("JDBC 连接失败", e);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String item = key.toString();
            int count = 0;
            int upCount = 0;
            for (Text v : values) {
                count++;
                String[] parts = v.toString().split(",");
                if (parts.length > 1 && "1".equals(parts[1])) upCount++;
            }
            context.getCounter("Gacha", "TOTAL_FIVE").increment(count);
            context.getCounter("Gacha", "TOTAL_UP").increment(upCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            long totalFive = context.getCounter("Gacha", "TOTAL_FIVE").getValue();
            long totalUp = context.getCounter("Gacha", "TOTAL_UP").getValue();
            long totalPulls = context.getConfiguration().getLong("gacha.total.pulls", 5000);

            try {
                // 清理 7 天前数据
                PreparedStatement del = conn.prepareStatement(
                    "DELETE FROM ads_gacha_offline WHERE batch_time < NOW() - INTERVAL 7 DAY");
                del.executeUpdate();

                // 写入汇总行
                String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                double fiveRate = totalPulls > 0 ? (double) totalFive / totalPulls * 100 : 0;
                PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ads_gacha_offline (batch_time, item_name, item_type, five_star_count, total_pulls, five_rate, pity_triggered) VALUES (?,?,?,?,?,?,?)");
                ps.setString(1, now);
                ps.setString(2, "__ALL__");
                ps.setString(3, "summary");
                ps.setLong(4, totalFive);
                ps.setLong(5, totalPulls);
                ps.setDouble(6, Math.round(fiveRate * 100.0) / 100.0);
                ps.setInt(7, 0);
                ps.executeUpdate();

                conn.commit();
                ps.close();
                del.close();
                conn.close();

                System.out.println("\n========================================");
                System.out.println("  Gacha DWS 聚合完成 → MySQL ads_gacha_offline");
                System.out.println("  总抽数: " + totalPulls + " | 五星: " + totalFive + " | 五星率: " + String.format("%.2f", fiveRate) + "%");
                System.out.println("========================================\n");
            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignored) {}
                throw new IOException("MySQL 写入失败", e);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("用法: GachaAggMR <DWD输入路径> [--total-pulls N]");
            return 1;
        }

        Configuration conf = getConf();

        // 解析总抽数参数
        long totalPulls = 5000;
        for (int i = 0; i < args.length; i++) {
            if ("--total-pulls".equals(args[i]) && i + 1 < args.length) {
                totalPulls = Long.parseLong(args[i + 1]);
                break;
            }
        }
        conf.setLong("gacha.total.pulls", totalPulls);

        Job job = Job.getInstance(conf, "Gacha DWS — Aggregate to MySQL");
        job.setJarByClass(GachaAggMR.class);

        job.setMapperClass(GachaAggMapper.class);
        job.setReducerClass(GachaAggReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outPath = new Path(args[0] + "/_agg_tmp");
        outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GachaAggMR(), args));
    }
}
