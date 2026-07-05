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

/**
 * 逐风数据洞察平台 — 抽卡统计 MapReduce 作业
 *
 * 功能: 从抽卡CSV中统计各角色/武器的总抽取次数
 *
 * 输入: HDFS /data/gacha/gacha_records.csv
 * 输出: HDFS /output/gacha/
 *
 * 提交命令:
 *   hadoop jar gacha-mr.jar GachaDriver \
 *     /data/gacha/gacha_records.csv \
 *     /output/gacha
 *
 * 覆盖技术点: MapReduce, HDFS, YARN
 */
public class GachaDriver extends Configured implements Tool {

    /**
     * Mapper: 解析CSV行 → (角色名, 1)
     * CSV格式: uid, banner, item, star, timestamp, pity_count
     */
    public static class GachaMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text charName = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // 跳过表头
            if (line.startsWith("uid") || line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length >= 3) {
                String item = fields[2].trim();  // 第3列: item(角色名/武器名)
                if (!item.isEmpty()) {
                    charName.set(item);
                    context.write(charName, one);
                }
            }
        }
    }

    /**
     * Reducer: 聚合抽取次数 → (角色名, 总次数)
     */
    public static class GachaReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: hadoop jar gacha-mr.jar GachaDriver <输入路径> <输出路径>");
            System.err.println("示例: hadoop jar gacha-mr.jar GachaDriver /data/gacha/gacha_records.csv /output/gacha");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "GachaPullCounter");

        job.setJarByClass(GachaDriver.class);
        job.setMapperClass(GachaMapper.class);
        job.setReducerClass(GachaReducer.class);

        // 可选Combiner：本地预聚合，减少网络传输
        job.setCombinerClass(GachaReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GachaDriver(), args);
        System.exit(exitCode);
    }
}
