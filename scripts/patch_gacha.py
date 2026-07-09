#!/usr/bin/env python3
"""Replace hardcoded banner data with API fetch in 历代卡池抽取比例.html"""
import re

path = "D:/neu2026bd/projects/ruoyi/ruoyi-ui/public/analysis/历代卡池抽取比例.html"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Find the script block with const ROLE_LIST through const HYBRID_LIST
# End of hardcoded data is before the </script> that contains render logic
old_data_start = content.find('// ==================== 原始数据')
old_data_end = content.find('</script>', old_data_start)

if old_data_start == -1:
    print("ERROR: data section not found")
    exit(1)

# Keep the render logic (after the data arrays), just replace the data section
# The data section ends right after HYBRID_LIST definition
hybrid_end = content.find('];', content.find('const HYBRID_LIST')) + 2

new_script = """// ==================== 数据从 API 加载 ====================
var ROLE_LIST = [];
var WEAPON_LIST = [];
var HYBRID_LIST = [];

async function loadData() {
  try {
    var res = await fetch('/analysis/gacha/banners?page=1&size=500');
    var json = await res.json();
    // Group by version
    var verMap = {};
    json.data.rows.forEach(function(r) {
      var v = r.version_name;
      if (!verMap[v]) verMap[v] = { version: v, start: r.start_time, end: r.end_time, content: {}, imgList: [] };
      verMap[v].content[r.char_name] = (verMap[v].content[r.char_name] || 0) + parseInt(r.pull_count || 0);
    });
    // Sort by version desc
    var vers = Object.keys(verMap).sort().reverse();
    vers.forEach(function(v) {
      ROLE_LIST.push(verMap[v]);
    });
    document.getElementById('total-periods').textContent = ROLE_LIST.length;
    renderAll();
  } catch(e) {
    document.getElementById('cards-container').innerHTML = '<div style=\"padding:40px;color:#f85149;text-align:center\">数据加载失败，请确认后端已启动</div>';
    console.error(e);
  }
}

"""

# Replace from old_data_start to the first const ROLE_LIST line onwards
const_role_start = content.find('const ROLE_LIST')
content = content[:const_role_start] + new_script + content[hybrid_end+1:]

# Replace the init call at the end
content = content.replace('document.getElementById(\'total-periods\').textContent = ROLE_LIST.length;\nrenderAll();',
                          'loadData();')

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("Gacha HTML updated successfully")
