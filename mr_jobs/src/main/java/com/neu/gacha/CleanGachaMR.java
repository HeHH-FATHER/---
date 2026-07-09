package com.neu.gacha;

import java.io.IOException;
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
 * 逐风数据洞察平台 — DWD 层 抽卡数据清洗 MR
 * ═══════════════════════════════════════════════════════════════
 * 功能: 读取 ODS 层逐抽 JSONL，过滤仅保留 5★ 出货记录。
 *
 * 输入: HDFS JSONL — 每行一条抽卡记录 {"star":3|4|5, "item":"...", "type":"role"|"weapon", "is_up":bool}
 * 输出: HDFS JSONL — 仅 5★ 记录
 *
 * 用法:
 *   hadoop jar gacha-mr.jar com.neu.gacha.CleanGachaMR /data/gacha/ods/ /data/gacha/dwd/
 */
public class CleanGachaMR extends Configured implements Tool {

    public static class GachaFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private final JsonParser parser = new JsonParser();
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            try {
                JsonObject obj = parser.parse(line).getAsJsonObject();
                int star = obj.has("star") ? obj.get("star").getAsInt() : 3;
                if (star == 5) {
                    outKey.set(line);
                    context.write(outKey, NullWritable.get());
                }
            } catch (Exception e) {
                context.getCounter("Gacha", "PARSE_ERROR").increment(1);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: CleanGachaMR <输入路径> <输出路径>");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Gacha DWD — Clean 5★ Filter");
        job.setJarByClass(CleanGachaMR.class);

        job.setMapperClass(GachaFilterMapper.class);
        job.setNumReduceTasks(0);  // Map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            long in = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
            long out = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            System.out.println("\n========================================");
            System.out.println("  Gacha DWD 清洗完成");
            System.out.println("  输入: " + in + " 抽 → 输出: " + out + " 五星");
            System.out.println("========================================\n");
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CleanGachaMR(), args));
    }
}
