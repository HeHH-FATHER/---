/**
 * verify.js — 深渊数据生成器验证脚本
 *
 * 读取生成器产出的用户 JSON 文件，聚合后与源统计数据对比，输出偏差报告。
 * 同时检查数据一致性（uid 匹配、配队角色都在 BOX 内、两队无重叠）。
 *
 * ── 用法 ──
 *   node verify.js <输出目录> <源统计JSON路径>
 *
 * ── 示例 ──
 *   # 先运行生成器
 *   java -jar target/test1-1.0-SNAPSHOT-jar-with-dependencies.jar --stats "src/test/java/org/example/v59_6.6深渊使用率统计（第一期） - 副本.json" --out target/test_output --users 300 --seed 42
 *
 *   # 再验证
 *   node verify.js target/test_output "src/test/java/org/example/v59_6.6深渊使用率统计（第一期） - 副本.json"
 *
 * ── 依赖 ──
 *   仅需 Node.js（无需额外安装包），支持 Windows / macOS / Linux
 */

const fs = require('fs');
const path = require('path');

// ═══════════════════════════════════════
// 1. 解析命令行参数
// ═══════════════════════════════════════
const outDir = process.argv[2];
const statsPath = process.argv[3];

if (!outDir || !statsPath) {
  console.error('用法: node verify.js <输出目录> <源统计JSON路径>');
  console.error('示例: node verify.js target/test_output "src/test/java/org/example/xxx.json"');
  process.exit(1);
}

// ═══════════════════════════════════════
// 2. 加载源统计数据（作为对比基准）
// ═══════════════════════════════════════
const stats = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
console.log('已加载源统计: ' + stats.title + ' (样本数: ' + stats.samples + ')');

// ═══════════════════════════════════════
// 3. 遍历输出目录，聚合所有用户数据
// ═══════════════════════════════════════
const boxFiles = fs.readdirSync(outDir).filter(f => f.endsWith('_角色box.json'));
const totalUsers = boxFiles.length;

if (totalUsers === 0) {
  console.error('错误: 输出目录中没有找到 _角色box.json 文件');
  console.error('请确认生成器已正常运行，且输出目录路径正确: ' + outDir);
  process.exit(1);
}

// 聚合计数器
const ownedCounts = new Map();   // 角色名 → 拥有该角色的用户数
const constSums = new Map();     // 角色名 → 命座总和（用于算均值）
const teamUsage = new Map();     // "队A,队B" → 使用次数

// 数据一致性计数器
let missingRecords = 0;          // 有 BOX 但缺少战绩的
let uidMismatchCount = 0;        // uid 不一致的
let missingTeamCharCount = 0;    // 配队角色不在 BOX 的
let teamOverlapCount = 0;        // 两队有角色重叠的

for (const boxFile of boxFiles) {
  const uid = boxFile.replace('_角色box.json', '');
  const recordFile = uid + '_深渊战绩.json';

  // 检查战绩文件是否存在
  if (!fs.existsSync(path.join(outDir, recordFile))) {
    missingRecords++;
    continue;
  }

  const box = JSON.parse(fs.readFileSync(path.join(outDir, boxFile), 'utf8'));
  const record = JSON.parse(fs.readFileSync(path.join(outDir, recordFile), 'utf8'));

  // ── 一致性检查 ①: uid 必须一致 ──
  if (box.uid !== record.uid) {
    uidMismatchCount++;
  }

  // ── 聚合: 角色拥有 & 命座 ──
  for (const c of box.characters) {
    ownedCounts.set(c.name, (ownedCounts.get(c.name) || 0) + 1);
    constSums.set(c.name, (constSums.get(c.name) || 0) + c.constellation);
  }

  // ── 一致性检查 ②: 配队角色必须在 BOX 中 ──
  const ownedNames = new Set(box.characters.map(c => c.name));
  for (const t of record.teams) {
    for (const m of t.members) {
      if (!ownedNames.has(m.name)) {
        missingTeamCharCount++;
      }
    }
  }

  // ── 聚合: 配队使用 ──
  const ids = record.teams.map(t => t.team_index).sort((a, b) => a - b).join(',');
  teamUsage.set(ids, (teamUsage.get(ids) || 0) + 1);

  // ── 一致性检查 ③: 两队不能有角色重叠 ──
  if (record.teams.length === 2) {
    const set1 = new Set(record.teams[0].members.map(m => m.name));
    const set2 = new Set(record.teams[1].members.map(m => m.name));
    const overlap = [...set1].filter(x => set2.has(x));
    if (overlap.length > 0) {
      teamOverlapCount++;
    }
  }
}

// ═══════════════════════════════════════
// 4. 输出验证报告
// ═══════════════════════════════════════
console.log('');
console.log('═══════════════════════════════════════');
console.log('  验证报告');
console.log('═══════════════════════════════════════');
console.log('生成用户数: ' + totalUsers);
console.log('缺失战绩文件: ' + missingRecords);
console.log('');

// ── 一致性检查结果 ──
console.log('── 数据一致性 ──');
console.log('uid 不匹配:      ' + (uidMismatchCount === 0 ? '✓ 无' : '✗ ' + uidMismatchCount + ' 处'));
console.log('配队角色缺失:     ' + (missingTeamCharCount === 0 ? '✓ 无' : '✗ ' + missingTeamCharCount + ' 处'));
console.log('队伍角色重叠:     ' + (teamOverlapCount === 0 ? '✓ 无' : '✗ ' + teamOverlapCount + ' 处'));
console.log('');

// ── 拥有率偏差对比 ──
// 对每个角色，计算「生成拥有率 - 源拥有率」的偏差
console.log('── 拥有率偏差 TOP 15（按 |偏差| 降序）──');
console.log('角色'.padEnd(12) + '星级'.padEnd(5) + '源拥有率'.padEnd(10) + '生成拥有率'.padEnd(11) + '偏差');

const deltas = [];
for (const c of stats.chars) {
  const owned = ownedCounts.get(c.name) || 0;
  const genRate = (owned / totalUsers * 100);
  deltas.push({
    name: c.name, star: c.star,
    src: c.own_rate, gen: genRate,
    delta: genRate - c.own_rate
  });
}

// 按绝对偏差从大到小排列
const sortedByAbsDelta = [...deltas].sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
for (const d of sortedByAbsDelta.slice(0, 15)) {
  console.log(
    d.name.padEnd(12) +
    ('☆' + d.star).padEnd(5) +
    d.src.toFixed(1) + '%'.padEnd(9) +
    d.gen.toFixed(1) + '%'.padEnd(10) +
    (d.delta >= 0 ? '+' : '') + d.delta.toFixed(1) + '%'
  );
}

const avgAbsDelta = deltas.reduce((s, d) => s + Math.abs(d.delta), 0) / deltas.length;
const maxAbsDelta = Math.max(...deltas.map(d => Math.abs(d.delta)));
console.log('平均绝对偏差: ' + avgAbsDelta.toFixed(1) + '%');
console.log('最大绝对偏差: ' + maxAbsDelta.toFixed(1) + '%');
console.log('');

// ── 命座均值对比（选取代表性角色）──
console.log('── 命座均值对比 ──');
console.log('角色'.padEnd(10) + '源均值'.padEnd(8) + '生成均值'.padEnd(10) + '拥有数/总数');

const keyChars = [
  '班尼特', '香菱', '行秋', '菲谢尔',       // 热门四星
  '玛薇卡', '芙宁娜', '丝柯克', '希诺宁',   // 热门限定五星
  '莫娜', '刻晴', '迪卢克',                 // 常驻五星
  '杜林', '尼可', '茜特菈莉'                // 高出场率角色
];
for (const name of keyChars) {
  const src = stats.chars.find(c => c.name === name);
  if (!src) continue;
  const owned = ownedCounts.get(name) || 0;
  const sum = constSums.get(name) || 0;
  const genAvg = owned > 0 ? (sum / owned) : 0;
  console.log(
    name.padEnd(10) +
    src.constellation.toFixed(1).padEnd(8) +
    genAvg.toFixed(2).padEnd(10) +
    owned + '/' + totalUsers
  );
}
console.log('');

// ── 配队使用分布 TOP 10 ──
console.log('── 配队组合使用次数 TOP 10 ──');
const topTeams = [...teamUsage.entries()]
  .map(([k, v]) => ({ key: k, count: v, pct: (v / totalUsers * 100) }))
  .sort((a, b) => b.count - a.count)
  .slice(0, 10);

for (const t of topTeams) {
  const [a, b] = t.key.split(',').map(Number);
  const t1 = stats.teams[a - 1];
  const t2 = stats.teams[b - 1];
  const n1 = t1.members.map(m => m.name).join(' + ');
  const n2 = t2.members.map(m => m.name).join(' + ');
  console.log(
    '[Team #' + t.key.padEnd(6) + '] ' +
    t.count + ' 次' +
    ' (' + t.pct.toFixed(1) + '%)'
  );
  console.log('  上半: ' + n1 + '  (源使用率: ' + t1.use_rate + '%)');
  console.log('  下半: ' + n2 + '  (源使用率: ' + t2.use_rate + '%)');
}

console.log('');
console.log('═══════════════════════════════════════');
console.log('  验证完成');
console.log('═══════════════════════════════════════');
