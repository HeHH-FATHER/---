#!/usr/bin/env python3
content = r'''<template>
  <div class="dark-dashboard">
    <div class="main">
      <div class="kpi-row">
        <div class="kpi-card"><span class="kpi-label">统计角色数</span><span class="kpi-val">{{ kpi.count || 0 }}</span></div>
        <div class="kpi-card"><span class="kpi-label">平均命座</span><span class="kpi-val" style="color:#a371f7">{{ kpi.avgConst || 0 }}</span></div>
        <div class="kpi-card"><span class="kpi-label">平均伤害</span><span class="kpi-val" style="color:#f0883e">{{ (kpi.avgDmg/10000).toFixed(1) }}万</span></div>
        <div class="kpi-card"><span class="kpi-label">快照时间</span><span class="kpi-val" style="font-size:16px;color:#3fb950">{{ kpi.snapshot || '--' }}</span></div>
      </div>

      <div class="top-bar">
        <input v-model="search" class="dark-input" placeholder="搜索角色..." style="width:200px" />
        <select v-model="sortBy" class="dark-select" @change="sortData">
          <option value="count">按样本数</option>
          <option value="damage">按伤害</option>
          <option value="constellation">按命座</option>
        </select>
        <span class="info-text">共 {{ filtered.length }} 名角色 · 实时窗口快照</span>
      </div>

      <div class="card-grid">
        <div class="build-card" v-for="c in filtered" :key="c.role_name">
          <div class="card-header">
            <img :src="c.avatar" class="card-avatar" @error="e=>e.target.style.display='none'" />
            <div class="card-info">
              <div class="card-name">{{ c.role_name }} <span :class="c.star===5?'s5':'s4'">{{ '★'.repeat(c.star) }}</span></div>
              <div class="card-dmg">{{ (c.avg_damage/10000).toFixed(1) }}万 <span class="lbl">均伤</span></div>
            </div>
          </div>
          <div class="card-stats">
            <div class="stat"><span class="lbl">样本</span><span class="val">{{ c.count }}</span></div>
            <div class="stat"><span class="lbl">命座</span><span class="val">{{ c.avg_constellation }}</span></div>
          </div>
          <div class="card-divider"></div>
          <div class="card-section-title">武器</div>
          <div class="item-row" v-if="c.top_weapon && c.top_weapon!=='?'">
            <img :src="c.top_weapon_icon" class="item-icon" @error="e=>e.target.style.display='none'" />
            <div class="item-info"><div class="item-name">{{ c.top_weapon }}</div>
              <div class="item-bar"><div class="bar-fill wpn" :style="{width:(c.top_weapon_ratio*100)+'%'}"></div></div>
            </div>
            <span class="item-rate">{{ (c.top_weapon_ratio*100).toFixed(0) }}%</span>
          </div>
          <div class="card-divider"></div>
          <div class="card-section-title">圣遗物</div>
          <div class="item-row" v-if="c.top_artifact && c.top_artifact!=='?'">
            <img :src="c.top_artifact_icon" class="item-icon" @error="e=>e.target.style.display='none'" />
            <div class="item-info"><div class="item-name">{{ c.top_artifact }}</div>
              <div class="item-bar"><div class="bar-fill art" :style="{width:(c.top_artifact_ratio*100)+'%'}"></div></div>
            </div>
            <span class="item-rate">{{ (c.top_artifact_ratio*100).toFixed(0) }}%</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import request from '@/utils/request'

export default {
  name: 'BuildAnalysis',
  data() {
    return { allData:[], kpi:{}, search:'', sortBy:'count', snapshot:'' }
  },
  computed: {
    filtered() {
      let d = this.allData
      if (this.search) { const s=this.search.toLowerCase(); d=d.filter(x=>x.role_name.toLowerCase().includes(s)) }
      if (this.sortBy==='count') d=[...d].sort((a,b)=>b.count-a.count)
      else if (this.sortBy==='damage') d=[...d].sort((a,b)=>b.avg_damage-a.avg_damage)
      else d=[...d].sort((a,b)=>b.avg_constellation-a.avg_constellation)
      return d
    }
  },
  async mounted() {
    try {
      const r = await request({ url:'/analysis/build/hot', method:'get' })
      const rows = r.data||[]
      // 取最新快照
      if (rows.length) {
        const latest = rows[0].snapshot_time
        this.allData = rows.filter(x=>x.snapshot_time===latest)
        this.snapshot = latest
      }
      const total = this.allData.length
      const avgC = total? (this.allData.reduce((s,x)=>s+(x.avg_constellation||0),0)/total).toFixed(1) : 0
      const avgD = total? (this.allData.reduce((s,x)=>s+(x.avg_damage||0),0)/total).toFixed(0) : 0
      this.kpi = { count:total, avgConst:avgC, avgDmg:avgD, snapshot:this.snapshot?.slice(0,19) }
    } catch(e) {}
  },
  methods: { sortData() {} }
}
</script>

<style scoped>
* { margin:0; padding:0; box-sizing:border-box; }
.dark-dashboard { background:#0a0e17; color:#c8d6e5; min-height:100vh; font-family:'Microsoft YaHei','PingFang SC',sans-serif; }
.main { max-width:1600px; margin:0 auto; padding:16px 20px; }
.kpi-row { display:flex; gap:14px; margin-bottom:14px; }
.kpi-card { flex:1; background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:16px 20px; text-align:center; }
.kpi-card:hover { border-color:#30363d; }
.kpi-label { display:block; font-size:12px; color:#8b949e; margin-bottom:6px; }
.kpi-val { display:block; font-size:22px; font-weight:700; color:#e6edf3; }
.top-bar { display:flex; gap:12px; align-items:center; margin-bottom:14px; }
.dark-input { padding:7px 12px; border-radius:6px; border:1px solid #21262d; background:#0d1117; color:#e6edf3; font-size:12px; outline:none; }
.dark-input:focus { border-color:#58a6ff; }
.dark-select { padding:7px 12px; border-radius:6px; border:1px solid #21262d; background:#0d1117; color:#c8d6e5; font-size:12px; outline:none; cursor:pointer; }
.info-text { font-size:12px; color:#8b949e; margin-left:auto; }
.card-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
@media (max-width:1400px) { .card-grid { grid-template-columns:repeat(3,1fr); } }
@media (max-width:900px) { .card-grid { grid-template-columns:repeat(2,1fr); } }
.build-card { background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:14px; transition:border-color .2s; }
.build-card:hover { border-color:#30363d; }
.card-header { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
.card-avatar { width:48px; height:48px; border-radius:50%; object-fit:cover; background:#161b22; border:2px solid #21262d; flex-shrink:0; }
.card-info { flex:1; min-width:0; }
.card-name { font-size:14px; font-weight:700; color:#e6edf3; } .card-name .s5 { color:#d29922; font-size:10px; } .card-name .s4 { color:#a371f7; font-size:10px; }
.card-dmg { font-size:13px; color:#f0883e; font-weight:600; } .card-dmg .lbl { font-size:10px; color:#8b949e; font-weight:400; }
.card-stats { display:flex; gap:16px; margin-bottom:6px; }
.stat { font-size:11px; } .stat .lbl { color:#8b949e; } .stat .val { color:#e6edf3; font-weight:600; margin-left:4px; }
.card-divider { height:1px; background:#161b22; margin:6px 0; }
.card-section-title { font-size:10px; color:#8b949e; margin-bottom:2px; }
.item-row { display:flex; align-items:center; gap:8px; padding:3px 0; }
.item-icon { width:24px; height:24px; border-radius:4px; object-fit:cover; background:#161b22; flex-shrink:0; }
.item-info { flex:1; min-width:0; }
.item-name { font-size:11px; color:#c8d6e5; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.item-bar { height:4px; background:#161b22; border-radius:2px; margin-top:2px; }
.bar-fill { height:100%; border-radius:2px; }
.bar-fill.wpn { background:linear-gradient(90deg,#58a6ff,#79c0ff); }
.bar-fill.art { background:linear-gradient(90deg,#3fb950,#7ee787); }
.item-rate { font-size:10px; color:#8b949e; width:30px; text-align:right; flex-shrink:0; }
::-webkit-scrollbar { width:4px; } ::-webkit-scrollbar-thumb { background:#21262d; border-radius:2px; }
</style>
'''
with open('D:/neu2026bd/projects/ruoyi/ruoyi-ui/src/views/analysis/build/index.vue','w',encoding='utf-8') as f:
    f.write(content)
print('done')
