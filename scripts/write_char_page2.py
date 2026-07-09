#!/usr/bin/env python3
content = r'''<template>
  <div class="dark-dashboard">
    <div class="main">
      <div class="kpi-row">
        <div class="kpi-card"><span class="kpi-label">总角色数</span><span class="kpi-val">{{ summary.total || 0 }}</span></div>
        <div class="kpi-card"><span class="kpi-label">最高使用率</span><span class="kpi-val" style="color:#d29922">{{ summary.top_char || '-' }}</span><span class="kpi-sub">{{ summary.max_use }}%</span></div>
        <div class="kpi-card"><span class="kpi-label">最高持有率</span><span class="kpi-val" style="color:#58a6ff">{{ summary.top_own || '-' }}</span></div>
        <div class="kpi-card"><span class="kpi-label">数据版本</span><span class="kpi-val" style="font-size:16px;color:#3fb950">{{ currentVer || '--' }}</span></div>
      </div>
      <div class="ver-row">
        <button v-for="(v,i) in versions" :key="v" :class="['ver-tab',{active:i===verIdx}]" @click="switchVer(i)">{{ v.replace('深渊使用率统计','').replace('(第一期)','①').replace('(第二期)','②') }}</button>
      </div>

      <!-- 第一行: 散点图(象限) + 元素趋势 -->
      <div class="chart-row">
        <div class="panel" style="flex:1.3">
          <div class="panel-title">使用率 × 持有率 · 角色定位象限</div>
          <div ref="scatterChart" style="height:400px"></div>
          <div class="quadrant-legend">
            <span class="q-tag" style="color:#3fb950">▲ 人权卡：低持有高使用</span>
            <span class="q-tag" style="color:#d29922">★ 版本之子：高持有高使用</span>
            <span class="q-tag" style="color:#8b949e">◆ 仓库管理：高持有低使用</span>
            <span class="q-tag" style="color:#484f58">○ 边缘角色：低持有低使用</span>
          </div>
        </div>
        <div class="panel" style="flex:1">
          <div class="panel-title">七元素使用率变迁</div>
          <div ref="elemTrendChart" style="height:400px"></div>
        </div>
      </div>

      <!-- 第二行: 命座vs使用 + 涨跌 -->
      <div class="chart-row" style="margin-top:14px">
        <div class="panel" style="flex:1">
          <div class="panel-title">命座投入 vs 使用率回报 · 陷阱检测</div>
          <div ref="consChart" style="height:350px"></div>
        </div>
        <div class="panel" style="flex:1">
          <div class="panel-title">版本涨跌 TOP10</div>
          <div ref="momentumChart" style="height:350px"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import * as echarts from 'echarts'
import { getCharSummary, getCharRanking, getCharScatter } from '@/api/analysis/char'
import request from '@/utils/request'

export default {
  name: 'CharAnalysis',
  data() {
    return { summary:{}, currentVer:'', versions:[], verIdx:0 }
  },
  async mounted() {
    try { const r=await request({url:'/analysis/char/versions',method:'get'}); this.versions=r.data.map(v=>v.version_name) } catch(e) {}
    if (this.versions.length) this.switchVer(0)
    this.loadSummary()
    this.loadElementTrend()
    this.loadMomentum()
  },
  methods: {
    async switchVer(i) { this.verIdx=i; this.currentVer=this.versions[i]; this.loadScatter(); this.loadConsChart() },
    async loadSummary() { const r=await getCharSummary(); this.summary=r.data },
    async loadScatter() {
      const r=await getCharScatter(); const d=r.data, c=echarts.init(this.$refs.scatterChart)
      const avgUse=d.reduce((s,x)=>s+x.use_rate,0)/d.length, avgOwn=d.reduce((s,x)=>s+x.own_rate,0)/d.length
      c.setOption({ tooltip:{trigger:'item',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11},formatter:p=>'<b>'+p.data[2]+'</b><br/>使用率:'+p.data[0]+'%<br/>持有率:'+p.data[1]+'%'},
        xAxis:{name:'使用率(%)',nameTextStyle:{color:'#8b949e'},max:100,axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}},
        yAxis:{name:'持有率(%)',nameTextStyle:{color:'#8b949e'},max:100,axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}},
        series:[{type:'scatter',symbolSize:d=>d[3]===5?12:8,data:d.map(d=>[d.use_rate,d.own_rate,d.char_name,d.star]),itemStyle:{color:d=>d[3]===5?'#f56c6c':'#58a6ff'},
          markLine:{silent:true,data:[{xAxis:avgUse,lineStyle:{color:'#484f58',type:'dashed'}},{yAxis:avgOwn,lineStyle:{color:'#484f58',type:'dashed'}}]} }],
        grid:{top:20,bottom:40,left:50,right:20} })
    },
    async loadElementTrend() {
      try { const r=await request({url:'/analysis/char/element-trend',method:'get'}); const d=r.data
      const c=echarts.init(this.$refs.elemTrendChart)
      const colors={火:'#f85149',水:'#58a6ff',风:'#3fb950',雷:'#a371f7',冰:'#79c0ff',岩:'#d29922',草:'#7ee787'}
      c.setOption({ tooltip:{trigger:'axis',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11}},
        legend:{bottom:0,textStyle:{color:'#8b949e',fontSize:10}},
        grid:{top:10,bottom:35,left:40,right:20},
        xAxis:{type:'category',data:d.versions.map(v=>v.replace('深渊使用率统计','').replace('(第一期)','①').replace('(第二期)','②')),axisLabel:{color:'#8b949e',fontSize:9},axisLine:{lineStyle:{color:'#21262d'}}},
        yAxis:{type:'value',name:'平均使用率%',axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}}},
        series:d.series.map(s=>({name:s.name,type:'line',data:s.data,smooth:true,symbol:'circle',symbolSize:5,lineStyle:{width:2,color:colors[s.name]},itemStyle:{color:colors[s.name]},connectNulls:true}))
      }) } catch(e) {}
    },
    async loadConsChart() {
      try { const r=await getCharRanking({version:this.currentVer,page:1,size:200,sortBy:'use_rate'}); const d=r.data.rows, c=echarts.init(this.$refs.consChart)
      c.setOption({ tooltip:{trigger:'item',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11},formatter:p=>'<b>'+p.data[2]+'</b><br/>命座:'+p.data[0]+'<br/>使用率:'+p.data[1]+'%'},
        xAxis:{name:'平均命座',nameTextStyle:{color:'#8b949e'},axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}},
        yAxis:{name:'使用率(%)',nameTextStyle:{color:'#8b949e'},axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}},
        series:[{type:'scatter',symbolSize:d=>d[3]===5?10:7,data:d.map(x=>[x.avg_constellation,x.use_rate,x.char_name,x.star]),itemStyle:{color:d=>d[3]===5?'#f56c6c':'#58a6ff'} }],
        grid:{top:20,bottom:30,left:50,right:20} })
      } catch(e) {}
    },
    async loadMomentum() {
      try { const r=await request({url:'/analysis/momentum/list',method:'get'})
      const d=r.data.slice(0,10), c=echarts.init(this.$refs.momentumChart)
      c.setOption({ tooltip:{trigger:'axis',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11}},
        grid:{top:10,bottom:20,left:80,right:30}, xAxis:{type:'value',name:'涨跌 %',axisLabel:{color:'#8b949e'},splitLine:{lineStyle:{color:'#161b22'}},axisLine:{lineStyle:{color:'#21262d'}}},
        yAxis:{type:'category',data:d.map(x=>x.char_name).reverse(),axisLabel:{color:'#c8d6e5',fontSize:11},axisLine:{lineStyle:{color:'#21262d'}}},
        series:[{type:'bar',data:d.map(x=>({value:x.trend,itemStyle:{color:x.trend>0?'#3fb950':'#f85149'}})).reverse()}] })
      } catch(e) {}
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
.ver-row { display:flex; gap:6px; margin-bottom:14px; flex-wrap:wrap; }
.ver-tab { padding:6px 14px; border-radius:4px; border:1px solid #21262d; background:#0d1117; color:#8b949e; font-size:12px; cursor:pointer; transition:all .2s; }
.ver-tab:hover { border-color:#30363d; }
.ver-tab.active { background:#1a2b4a; border-color:#58a6ff; color:#58a6ff; font-weight:600; }
.quadrant-legend { display:flex; gap:16px; margin-top:8px; font-size:11px; justify-content:center; flex-wrap:wrap; }
.q-tag { display:inline-block; }
::-webkit-scrollbar { width:4px; } ::-webkit-scrollbar-thumb { background:#21262d; border-radius:2px; }
</style>
'''
with open('D:/neu2026bd/projects/ruoyi/ruoyi-ui/src/views/analysis/char/index.vue','w',encoding='utf-8') as f:
    f.write(content)
print('done')
