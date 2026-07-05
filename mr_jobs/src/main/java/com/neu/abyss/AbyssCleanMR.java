package com.neu.abyss;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * ═══════════════════════════════════════════════════════════════
 * 逐风数据洞察平台 — DWD 层 数据清洗 MR
 * ═══════════════════════════════════════════════════════════════
 *
 * 对应文档: 离线链/清洗规范.md（10 条校验规则）
 *
 * 层级流: ODS(HDFS JSONL) → DWD(HDFS CSV)
 * 下游:   DWS 层 AbyssAggMR
 *
 * 输入: HDFS /data/abyss/ods/abyss_*.jsonl
 *       每行: {"uid":"...","version":"v6.6","box":{...},"record":{...}}
 *
 * 输出:
 *   Clean → /data/abyss/dwd/
 *     dwd_char_detail.csv  版本,uid,角色,星级,命座,等级,是否上阵
 *     dwd_team_usage.csv   版本,uid,半场,队伍编号,角色,星级,位置
 *   Dirty → /data/abyss/dirty/{类型}/
 *
 * 提交命令:
 *   hadoop jar abyss-mr.jar com.neu.abyss.AbyssCleanMR \
 *     /data/abyss/ods/ \
 *     /data/abyss/output_clean
 *
 *   # 注意: 输出路径 /data/abyss/output_clean 为临时目录，
 *   #       MultipleOutputs 会将 clean/dirty 写入适当子目录
 *
 * @author 逐风数据洞察平台
 * @version 5.0 (JSON-input MR)
 */
public class AbyssCleanMR extends Configured implements Tool {

    // ═══════════════════════════════════════════
    // 常量
    // ═══════════════════════════════════════════

    /** 合法星级 */
    private static final Set<Integer> VALID_STARS = new HashSet<>(Arrays.asList(4, 5));

    /** MultipleOutputs 命名 */
    private static final String OUT_CHAR_DETAIL = "chardetail";    // dwd_char_detail.csv
    private static final String OUT_TEAM_USAGE  = "teamusage";     // dwd_team_usage.csv
    private static final String OUT_DIRTY       = "dirty";         // dirty/{type}/

    /** 脏数据类型标签 */
    private static final String[] DIRTY_TYPES = {
        "bad_star", "bad_const", "bad_level", "missing_field", "empty_box",
        "overlap", "missing_char", "bad_team_size", "bad_uid", "bad_teams"
    };

    // 计数器
    private enum Counter {
        TOTAL_USERS,
        CLEAN_PASSED,
        DIRTY_BAD_STAR,
        DIRTY_BAD_CONST,
        DIRTY_BAD_LEVEL,
        DIRTY_MISSING_FIELD,
        DIRTY_EMPTY_BOX,
        DIRTY_OVERLAP,
        DIRTY_MISSING_CHAR,
        DIRTY_BAD_TEAM_SIZE,
        DIRTY_BAD_UID,
        DIRTY_BAD_TEAMS,
        CLEAN_CHAR_ROWS,
        CLEAN_TEAM_ROWS
    }

    // ═══════════════════════════════════════════
    // Mapper
    // ═══════════════════════════════════════════

    public static class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private final ObjectMapper jsonMapper = new ObjectMapper();
        private MultipleOutputs<NullWritable, Text> mos;

        // 复用 Text 对象
        private final Text charRow = new Text();
        private final Text teamRow = new Text();
        private final Text dirtyLine = new Text();

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(Counter.TOTAL_USERS).increment(1);
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // ── 1. JSON 解析 ──
            JsonNode root;
            try {
                root = jsonMapper.readTree(line);
            } catch (Exception e) {
                routeDirty(context, "bad_uid", line);
                return;
            }

            String uid    = pathText(root, "uid");
            String version = pathText(root, "version");
            JsonNode box  = root.get("box");
            JsonNode record = root.get("record");

            // ── 规则9: uid 校验 ──
            if (uid == null || uid.isEmpty()) {
                routeDirty(context, "bad_uid", line);
                return;
            }
            if (box == null || record == null) {
                routeDirty(context, "bad_uid", line);
                return;
            }
            // BOX uid vs record uid 一致性
            String boxUid = pathText(box, "uid");
            String recUid = pathText(record, "uid");
            if (!uid.equals(boxUid) || !uid.equals(recUid)) {
                routeDirty(context, "bad_uid", line);
                return;
            }

            // ── 规则5: 空 BOX ──
            JsonNode characters = box.get("characters");
            if (characters == null || characters.size() == 0) {
                routeDirty(context, "empty_box", line);
                return;
            }

            // ── 规则10: teams 结构校验 ──
            JsonNode teams = record.get("teams");
            if (teams == null || teams.size() != 2) {
                routeDirty(context, "bad_teams", line);
                return;
            }

            // ── 2. 逐角色校验 BOX（规则1~4）──
            List<String[]> validChars = new ArrayList<>();
            boolean boxDirty = false;

            for (JsonNode c : characters) {
                String name = pathText(c, "name");
                int star    = c.path("star").asInt(-1);
                int cons    = c.path("constellation").asInt(-1);
                int level   = c.path("level").asInt(-1);

                // 规则4: name 缺失
                if (name == null || name.isEmpty()) {
                    routeDirty(context, "missing_field", line);
                    boxDirty = true;
                    break;
                }
                // 规则1: 星级合法
                if (!VALID_STARS.contains(star)) {
                    routeDirty(context, "bad_star", line);
                    boxDirty = true;
                    break;
                }
                // 规则2: 命座范围 0~6
                if (cons < 0 || cons > 6) {
                    routeDirty(context, "bad_const", line);
                    boxDirty = true;
                    break;
                }
                // 规则3: 等级范围 1~90
                if (level < 1 || level > 90) {
                    routeDirty(context, "bad_level", line);
                    boxDirty = true;
                    break;
                }

                validChars.add(new String[]{name, String.valueOf(star),
                        String.valueOf(cons), String.valueOf(level)});
            }
            if (boxDirty) return;  // BOX 脏，整条丢弃

            // 建立 BOX 角色集合（用于规则6/7）
            Set<String> boxNames = new HashSet<>();
            Map<String, int[]> charInfo = new LinkedHashMap<>(); // name → [star, cons, level]
            for (String[] ch : validChars) {
                boxNames.add(ch[0]);
                charInfo.put(ch[0], new int[]{
                    Integer.parseInt(ch[1]),
                    Integer.parseInt(ch[2]),
                    Integer.parseInt(ch[3])
                });
            }

            // ── 3. 逐队伍校验战绩（规则6~8）──
            JsonNode team1 = teams.get(0);
            JsonNode team2 = teams.get(1);

            // 规则10 子项: half 必须为 1 和 2
            int half1 = team1.path("half").asInt(-1);
            int half2 = team2.path("half").asInt(-1);
            if (half1 != 1 || half2 != 2) {
                routeDirty(context, "bad_teams", line);
                return;
            }

            // 收集上下半角色
            Set<String> half1Names = collectMemberNames(team1);
            Set<String> half2Names = collectMemberNames(team2);

            // 规则8: 队伍人数 1~4
            if (half1Names.isEmpty() || half1Names.size() > 4
                    || half2Names.isEmpty() || half2Names.size() > 4) {
                routeDirty(context, "bad_team_size", line);
                return;
            }

            // 规则6: 上下半角色不能重叠
            Set<String> overlap = new HashSet<>(half1Names);
            overlap.retainAll(half2Names);
            if (!overlap.isEmpty()) {
                routeDirty(context, "overlap", line);
                return;
            }

            // 规则7: 战绩角色必须在 BOX 中
            Set<String> allTeamNames = new HashSet<>();
            allTeamNames.addAll(half1Names);
            allTeamNames.addAll(half2Names);
            for (String tn : allTeamNames) {
                if (!boxNames.contains(tn)) {
                    routeDirty(context, "missing_char", line);
                    return;
                }
            }

            // ── 4. 通过全部校验 → 输出清洗后 CSV ──
            context.getCounter(Counter.CLEAN_PASSED).increment(1);

            // 4a. dwd_char_detail: 版本,uid,角色,星级,命座,等级,是否上阵
            Set<String> usedNames = new HashSet<>(half1Names);
            usedNames.addAll(half2Names);

            for (Map.Entry<String, int[]> e : charInfo.entrySet()) {
                String name = e.getKey();
                int[] info = e.getValue();
                int used = usedNames.contains(name) ? 1 : 0;

                charRow.set(String.format("%s,%s,%s,%d,%d,%d,%d",
                        version, uid, name, info[0], info[1], info[2], used));
                mos.write(OUT_CHAR_DETAIL, NullWritable.get(), charRow,
                        "dwd/char_detail/data");
                context.getCounter(Counter.CLEAN_CHAR_ROWS).increment(1);
            }

            // 4b. dwd_team_usage: 版本,uid,半场,队伍编号,角色,星级,位置
            writeTeamUsage(version, uid, team1, context);
            writeTeamUsage(version, uid, team2, context);
        }

        /** 将队伍成员展开写入 dwd_team_usage */
        private void writeTeamUsage(String version, String uid, JsonNode team,
                                     Context context) throws IOException, InterruptedException {
            int half = team.path("half").asInt();
            int teamIdx = team.path("team_index").asInt();
            JsonNode members = team.get("members");
            if (members == null) return;

            int pos = 1;
            for (JsonNode m : members) {
                String name = pathText(m, "name");
                int star = m.path("star").asInt(0);

                teamRow.set(String.format("%s,%s,%d,%d,%s,%d,%d",
                        version, uid, half, teamIdx, name, star, pos));
                mos.write(OUT_TEAM_USAGE, NullWritable.get(), teamRow,
                        "dwd/team_usage/data");
                context.getCounter(Counter.CLEAN_TEAM_ROWS).increment(1);
                pos++;
            }
        }

        /** 收集队伍成员名 */
        private Set<String> collectMemberNames(JsonNode team) {
            Set<String> names = new HashSet<>();
            JsonNode members = team.get("members");
            if (members != null) {
                for (JsonNode m : members) {
                    String n = pathText(m, "name");
                    if (n != null && !n.isEmpty()) names.add(n);
                }
            }
            return names;
        }

        /** 脏数据路由 → /data/abyss/dirty/{type}/ */
        private void routeDirty(Context context, String dirtyType, String rawLine)
                throws IOException, InterruptedException {

            // 计数器
            switch (dirtyType) {
                case "bad_star":      context.getCounter(Counter.DIRTY_BAD_STAR).increment(1); break;
                case "bad_const":     context.getCounter(Counter.DIRTY_BAD_CONST).increment(1); break;
                case "bad_level":     context.getCounter(Counter.DIRTY_BAD_LEVEL).increment(1); break;
                case "missing_field": context.getCounter(Counter.DIRTY_MISSING_FIELD).increment(1); break;
                case "empty_box":     context.getCounter(Counter.DIRTY_EMPTY_BOX).increment(1); break;
                case "overlap":       context.getCounter(Counter.DIRTY_OVERLAP).increment(1); break;
                case "missing_char":  context.getCounter(Counter.DIRTY_MISSING_CHAR).increment(1); break;
                case "bad_team_size": context.getCounter(Counter.DIRTY_BAD_TEAM_SIZE).increment(1); break;
                case "bad_uid":       context.getCounter(Counter.DIRTY_BAD_UID).increment(1); break;
                case "bad_teams":     context.getCounter(Counter.DIRTY_BAD_TEAMS).increment(1); break;
            }

            dirtyLine.set(rawLine);
            mos.write(OUT_DIRTY, NullWritable.get(), dirtyLine,
                    "dirty/" + dirtyType + "/data");
        }

        /** 安全获取 JSON 文本字段 */
        private static String pathText(JsonNode node, String field) {
            JsonNode child = node.get(field);
            return (child != null && !child.isNull()) ? child.asText() : null;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    // ═══════════════════════════════════════════
    // Driver
    // ═══════════════════════════════════════════

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("══════════════════════════════════════════");
            System.err.println("AbyssCleanMR — DWD 层 数据清洗");
            System.err.println("══════════════════════════════════════════");
            System.err.println("用法: hadoop jar abyss-mr.jar com.neu.abyss.AbyssCleanMR <输入> <输出>");
            System.err.println("  <输入>  HDFS ODS JSONL 路径, 如 /data/abyss/ods/");
            System.err.println("  <输出>  HDFS 临时输出路径, 如 /data/abyss/clean_tmp/");
            System.err.println("");
            System.err.println("产出目录:");
            System.err.println("  /data/abyss/clean_tmp/dwd/char_detail/  → dwd_char_detail.csv");
            System.err.println("  /data/abyss/clean_tmp/dwd/team_usage/   → dwd_team_usage.csv");
            System.err.println("  /data/abyss/clean_tmp/dirty/{type}/     → 脏数据JSON");
            System.err.println("══════════════════════════════════════════");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "AbyssCleanMR-DWD");

        job.setJarByClass(AbyssCleanMR.class);
        job.setMapperClass(CleanMapper.class);

        // Map-only job（无 Reducer）
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // ── 注册 MultipleOutputs ──
        MultipleOutputs.addNamedOutput(job, OUT_CHAR_DETAIL,
                TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, OUT_TEAM_USAGE,
                TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, OUT_DIRTY,
                TextOutputFormat.class, NullWritable.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ── 删除输出目录（避免 FileAlreadyExists）──
        org.apache.hadoop.fs.FileSystem fs =
                org.apache.hadoop.fs.FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean success = job.waitForCompletion(true);

        // ── 打印统计 ──
        if (success) {
            long total   = job.getCounters().findCounter(Counter.TOTAL_USERS).getValue();
            long clean   = job.getCounters().findCounter(Counter.CLEAN_PASSED).getValue();
            long charRows = job.getCounters().findCounter(Counter.CLEAN_CHAR_ROWS).getValue();
            long teamRows = job.getCounters().findCounter(Counter.CLEAN_TEAM_ROWS).getValue();

            System.out.println("══════════════════════════════════════════");
            System.out.println("DWD 清洗完成!");
            System.out.println("  总用户数:     " + total);
            System.out.println("  通过清洗:     " + clean);
            System.out.println("  脏数据:       " + (total - clean));
            System.out.println("  DWD字符行:    " + charRows);
            System.out.println("  DWD配队行:    " + teamRows);
            System.out.println("══════════════════════════════════════════");
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AbyssCleanMR(), args);
        System.exit(exitCode);
    }
}
