package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.generator.UserDataGenerator;
import org.example.model.StatsDocument;
import org.example.util.SeededRandom;

import java.io.File;
import java.io.IOException;

/**
 * 深渊数据生成器 —— 命令行入口。
 *
 * <p>读取聚合统计数据（stats.json），为指定数量的虚拟用户生成
 * 角色BOX和深渊战绩的数据，输出到本地文件。</p>
 *
 * <pre>
 * 用法:
 *   java -jar abyss-record-generator-jar-with-dependencies.jar \
 *       --stats &lt;源统计JSON路径&gt; \
 *       --out &lt;输出目录&gt; \
 *       [--users 10000] [--seed 42] [--dirty-rate 0.05] [--quiet]
 *
 * 参数说明:
 *   --stats   (必填) 源统计 JSON 文件路径
 *   --out     (必填) 生成 JSON 的输出目录
 *   --users   生成用户数量，默认 1000
 *   --seed    随机种子，默认取当前时间戳
 *   --quiet   静默模式，不打印进度
 * </pre>
 */
public class App {

    public static void main(String[] args) {
        // ── 解析命令行参数 ──
        String statsPath = null;
        String outPath = null;
        int userCount = 1000;
        long seed = System.currentTimeMillis();
        double dirtyRate = 0.0;
        boolean quiet = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--stats":
                    statsPath = args[++i];
                    break;
                case "--out":
                    outPath = args[++i];
                    break;
                case "--users":
                    userCount = Integer.parseInt(args[++i]);
                    break;
                case "--seed":
                    seed = Long.parseLong(args[++i]);
                    break;
                case "--dirty-rate":
                    dirtyRate = Double.parseDouble(args[++i]);
                    break;
                case "--quiet":
                    quiet = true;
                    break;
                default:
                    System.err.println("未知参数: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        // ── 参数校验 ──
        if (statsPath == null) {
            System.err.println("错误: --stats 为必填参数。");
            printUsage();
            System.exit(1);
        }
        if (outPath == null) {
            System.err.println("错误: --out 为必填参数。");
            printUsage();
            System.exit(1);
        }

        // ── 执行生成 ──
        try {
            run(statsPath, outPath, userCount, seed, dirtyRate, quiet);
        } catch (Exception e) {
            System.err.println("致命错误: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 核心执行流程：加载统计 → 初始化生成器 → 批量生成 → 写文件。
     */
    private static void run(String statsPath, String outPath, int userCount,
                            long seed, double dirtyRate, boolean quiet) throws IOException {

        // ── 1. 加载源统计 JSON ──
        if (!quiet) System.out.println("加载统计数据: " + statsPath);
        ObjectMapper mapper = new ObjectMapper();
        StatsDocument stats = mapper.readValue(new File(statsPath), StatsDocument.class);
        if (!quiet) {
            System.out.println("  已加载 " + stats.char_count + " 个角色, "
                    + stats.team_count + " 支队伍, " + stats.tier_count + " 个梯队");
        }

        // ── 2. 初始化生成器 ──
        SeededRandom rng = new SeededRandom(seed);
        UserDataGenerator generator = new UserDataGenerator(stats, rng, dirtyRate);
        if (!quiet) {
            System.out.println("  预计算 " + generator.getTeamPairCount() + " 个可行配队组合");
        }

        // ── 3. 准备输出目录 ──
        File outDir = new File(outPath);
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        // ── 4. 批量生成 ──
        if (!quiet) {
            System.out.println("开始生成 " + userCount + " 个用户 (seed=" + seed + ")");
            System.out.println("  输出: " + outDir.getAbsolutePath());
        }

        long startTime = System.currentTimeMillis();

        int uidBase = 100_000_000 + (int) (Math.abs(seed) % 100_000_000);

        for (int i = 0; i < userCount; i++) {
            String uid = String.valueOf(uidBase + i);

            // 生成单用户数据
            UserDataGenerator.UserData data = generator.generate(uid, i);

            // 写入本地文件
            generator.writeUserFiles(uid, data, outDir);

            if (!quiet && (i + 1) % Math.max(1, userCount / 10) == 0) {
                System.out.println("  进度: " + (i + 1) + "/" + userCount);
            }
        }

        // ── 5. 完成 ──
        long elapsed = System.currentTimeMillis() - startTime;
        if (!quiet) {
            System.out.println("完成! 生成 " + userCount + " 个用户, 耗时 "
                    + String.format("%.1f", elapsed / 1000.0) + " 秒");
            System.out.println("  脏数据率: " + String.format("%.1f", dirtyRate * 100) + "%");
            System.out.println("  脏数据用户: " + generator.getDirtyCount());
            System.out.println("输出目录: " + outDir.getAbsolutePath());
        }
    }

    private static void printUsage() {
        System.err.println("用法: java -jar abyss-record-generator.jar --stats <路径> --out <目录> [--users N] [--seed N] [--dirty-rate 0.05] [--quiet]");
        System.err.println("  --stats      源统计 JSON 文件路径 (必填)");
        System.err.println("  --out        输出目录 (必填)");
        System.err.println("  --users      生成用户数量 (默认: 1000)");
        System.err.println("  --seed       随机种子 (默认: 当前时间戳)");
        System.err.println("  --dirty-rate 脏数据比例 0.0~1.0 (默认: 0.0)");
        System.err.println("  --quiet      静默模式");
    }
}
