#!/usr/bin/env python3
content = r'''<template>
  <div class="dark-dashboard">
    <div class="main">
      <div class="kpi-row">
        <div class="kpi-card"><span class="kpi-label">总角色数</span><span class="kpi-val">{{ summary.total || 0 }}</span></div>
        <div class="kpi-card"><span class="kpi-label">最高使用率</span><span class="kpi-val" style="color:#d29922">{{ summary.top_char || '-' }}</span><span class="kpi-sub">{{ summary.max_use }}%</span></div>
        <div class="kpi-card"><span class="kpi-label">最高持有率</span><span class="kpi-val" style="color:#58a6ff">{{ summary.top_own || '-' }}</span></div>
        <div class="kpi-card"><span class="kpi-label">当前版本</span><span class="kpi-val" style="font-size:16px;color:#3fb950">{{ currentVer }}</span></div>
      </div>
      <div class="chart-row">
        <div class="panel" style="flex:1.2"><div class="panel-title">使用率 x 持有率 散点图</div><div ref="scatterChart" style="height:380px"></div></div>
        <div class="panel" style="flex:0.8"><div class="panel-title">星级分布</div><div ref="pieChart" style="height:380px"></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-title" style="display:flex;align-items:center;justify-content:space-between">
          <span>角色排名</span>
          <div style="display:flex;align-items:center;gap:8px">
            <input v-model="keyword" class="dark-input" placeholder="搜索角色..." @keyup.enter="loadRanking" style="width:160px" />
            <select v-model="sortBy" class="dark-select" @change="loadRanking"><option value="use_rate">按使用率</option><option value="own_rate">按持有率</option><option value="avg_constellation">按命座</option></select>
          </div>
        </div>
        <div class="rank-header"><span class="rh-num">#</span><span class="rh-name">角色</span><span class="rh-bar">占比</span><span class="rh-val">使用率</span><span class="rh-val">持有率</span></div>
        <div class="rank-list" v-loading="tableLoading" element-loading-background="rgba(0,0,0,0.4)">
          <div class="rank-item" v-for="(c,i) in tableData" :key="c.char_name">
            <span class="ri-num" :class="{top:i<3}">{{ (page-1)*size + i + 1 }}</span>
            <img :src="c.avatar" class="ri-avatar" @error="e=>e.target.style.display='none'" />
            <span class="ri-name">{{ c.char_name }}<span class="ri-star" :class="c.star===5?'s5':'s4'">{{ '★'.repeat(c.star) }}</span></span>
            <div class="ri-bars"><div class="ri-bar-track"><div class="ri-bar-own" :style="{width:(c.own_rate||0)+'%'}"><div class="ri-bar-use" :style="{width:Math.min(c.use_rate||0,100)+'%'}"></div></div></div></div>
            <span class="ri-use">{{ c.use_rate }}%</span>
            <span class="ri-own">{{ c.own_rate }}%</span>
          </div>
        </div>
        <div style="display:flex;justify-content:center;gap:4px;margin-top:10px"><button v-for="p in totalPages" :key="p" class="page-btn" :class="{active:p===page}" @click="page=p;loadRanking()">{{ p }}</button></div>
      </div>
    </div>
  </div>
</template>

<script>
import * as echarts from 'echarts'
import { getCharSummary, getCharRanking, getCharScatter, getCharPie } from '@/api/analysis/char'

export default {
  name: 'CharAnalysis',
  data() {
    return { summary:{}, currentVer:'v6.6', keyword:'', sortBy:'use_rate', page:1, size:30, total:0, tableData:[], tableLoading:false }
  },
  computed: { totalPages() { return Math.max(1, Math.ceil(this.total / this.size)) } },
  mounted() { this.loadSummary(); this.loadRanking(); this.loadScatter(); this.loadPie() },
  methods: {
    async loadSummary() { const r=await getCharSummary(); this.summary=r.data },
    async loadRanking() { this.tableLoading=true; const r=await getCharRanking({keyword:this.keyword,page:this.page,size:this.size,sortBy:this.sortBy}); this.tableData=r.data.rows; this.total=r.data.total; this.tableLoading=false },
    async loadScatter() {
      const r=await getCharScatter(); const d=r.data, c=echarts.init(this.$refs.scatterChart)
      c.setOption({ tooltip:{trigger:'item',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11},formatter:p=>'<b>'+p.data[2]+'</b><br/>使用率:'+p.data[0]+'%<br/>持有率:'+p.data[1]+'%'}, xAxis:{name:'使用率(%)',nameTextStyle:{color:'#8b949e'},max:100,axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}}, yAxis:{name:'持有率(%)',nameTextStyle:{color:'#8b949e'},max:100,axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}}, series:[{type:'scatter',symbolSize:d=>d[3]===5?12:8,data:d.map(d=>[d.use_rate,d.own_rate,d.char_name,d.star]),itemStyle:{color:d=>d[3]===5?'#f56c6c':'#58a6ff'}}], grid:{top:20,bottom:40,left:50,right:20} })
    },
    async loadPie() {
      const r=await getCharPie(), c=echarts.init(this.$refs.pieChart)
      c.setOption({ tooltip:{trigger:'item',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:12}}, legend:{bottom:0,textStyle:{color:'#8b949e',fontSize:10}}, series:[{type:'pie',radius:['40%','70%'],center:['50%','45%'],data:r.data.map(d=>({name:d.star+'星',value:d.cnt,itemStyle:{color:d.star===5?'#f56c6c':'#58a6ff'}})),label:{color:'#8b949e',formatter:'{b}: {c}个 ({d}%)'}}], backgroundColor:'transparent' })
    }
  }
}
</script>

<style scoped>
* { margin:0; padding:0; box-sizing:border-box; }
.dark-dashboard { background:#0a0e17; color:#c8d6e5; min-height:100vh; font-family:'Microsoft YaHei','PingFang SC',sans-serif; }
.main { max-width:1500px; margin:0 auto; padding:16px 20px; }
.kpi-row { display:flex; gap:14px; margin-bottom:14px; }
.kpi-card { flex:1; background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:16px 20px; text-align:center; transition:border-color .2s; }
.kpi-card:hover { border-color:#30363d; }
.kpi-label { display:block; font-size:12px; color:#8b949e; margin-bottom:6px; }
.kpi-val { display:block; font-size:22px; font-weight:700; color:#e6edf3; }
.kpi-sub { display:block; font-size:11px; color:#8b949e; margin-top:2px; }
.chart-row { display:flex; gap:14px; }
.panel { background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:14px 16px; }
.panel-title { font-size:14px; font-weight:600; color:#e6edf3; margin-bottom:10px; }
.dark-input { padding:7px 12px; border-radius:6px; border:1px solid #21262d; background:#0d1117; color:#e6edf3; font-size:12px; outline:none; }
.dark-input:focus { border-color:#58a6ff; }
.dark-select { padding:7px 12px; border-radius:6px; border:1px solid #21262d; background:#0d1117; color:#c8d6e5; font-size:12px; outline:none; cursor:pointer; }
.rank-header { display:flex; align-items:center; padding:8px 14px; border-bottom:1px solid #21262d; font-size:11px; color:#8b949e; }
.rh-num { width:32px; } .rh-name { flex:1; } .rh-bar { width:200px; text-align:center; } .rh-val { width:60px; text-align:right; }
.rank-list { max-height:500px; overflow-y:auto; }
.rank-item { display:flex; align-items:center; gap:10px; padding:6px 14px; border-bottom:1px solid #161b22; font-size:13px; transition:background .15s; }
.rank-item:hover { background:#161b22; }
.ri-num { width:32px; font-weight:700; color:#8b949e; } .ri-num.top { color:#d29922; }
.ri-avatar { width:36px; height:36px; border-radius:50%; object-fit:cover; background:#161b22; flex-shrink:0; }
.ri-name { flex:1; color:#e6edf3; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ri-star { font-size:10px; margin-left:4px; } .ri-star.s5 { color:#d29922; } .ri-star.s4 { color:#a371f7; }
.ri-bars { width:200px; }
.ri-bar-track { height:16px; background:#3a3f4a; border-radius:3px; border:1px solid #555b68; position:relative; overflow:hidden; }
.ri-bar-own { height:14px; position:absolute; left:1px; top:0; background:linear-gradient(90deg,#2a5a8a,#3a7abf); border-radius:2px; }
.ri-bar-use { height:9px; position:absolute; left:0; top:2px; background:linear-gradient(90deg,#d4a017,#f0c84a); border-radius:2px; }
.ri-use { width:60px; text-align:right; color:#d29922; font-weight:600; }
.ri-own { width:60px; text-align:right; color:#58a6ff; }
.page-btn { padding:5px 12px; border:1px solid #21262d; border-radius:4px; background:#0d1117; color:#8b949e; font-size:12px; cursor:pointer; }
.page-btn.active { background:#1a2b4a; border-color:#58a6ff; color:#58a6ff; font-weight:600; }
::-webkit-scrollbar { width:4px; } ::-webkit-scrollbar-thumb { background:#21262d; border-radius:2px; }
</style>
'''
with open('D:/neu2026bd/projects/ruoyi/ruoyi-ui/src/views/analysis/char/index.vue','w',encoding='utf-8') as f:
    f.write(content)
print('done')
