#!/usr/bin/env python3
content = r'''<template>
  <div class="dark-dashboard">
    <div class="main">
      <div class="kpi-row">
        <div class="kpi-card"><span class="kpi-label">卡池总期数</span><span class="kpi-val">{{ kpi.periods || 0 }}</span></div>
        <div class="kpi-card"><span class="kpi-label">累计抽取</span><span class="kpi-val" style="color:#d29922">{{ (kpi.totalPulls/10000).toFixed(0) }}万</span></div>
        <div class="kpi-card"><span class="kpi-label">最热角色</span><span class="kpi-val" style="color:#f85149;font-size:18px">{{ kpi.topChar || '-' }}</span><span class="kpi-sub">{{ (kpi.topPulls/10000).toFixed(0) }}万抽</span></div>
        <div class="kpi-card"><span class="kpi-label">最热版本</span><span class="kpi-val" style="color:#58a6ff;font-size:16px">{{ kpi.topVer || '-' }}</span></div>
      </div>

      <!-- 第一行: 版本流水 + TOP10排行 -->
      <div class="chart-row">
        <div class="panel" style="flex:1.3">
          <div class="panel-title">各版本卡池流水对比</div>
          <div ref="verChart" style="height:380px"></div>
        </div>
        <div class="panel" style="flex:0.7">
          <div class="panel-title">角色抽取 TOP10</div>
          <div ref="topChart" style="height:380px"></div>
        </div>
      </div>

      <!-- 第二行: 卡池类型比例 + 历史表 -->
      <div class="chart-row" style="margin-top:14px">
        <div class="panel" style="flex:0.6">
          <div class="panel-title">卡池类型分布</div>
          <div ref="typeChart" style="height:320px"></div>
        </div>
        <div class="panel" style="flex:1.4">
          <div class="panel-title">卡池历史记录</div>
          <el-table :data="bannerData" stripe v-loading="loading" size="small" max-height="320" header-cell-class-name="dark-th" cell-class-name="dark-td" :row-class-name="darkRow">
            <el-table-column prop="version_name" label="版本" width="100" />
            <el-table-column prop="banner_type" label="类型" width="70" align="center" />
            <el-table-column prop="char_name" label="UP角色/武器" min-width="120" />
            <el-table-column prop="pull_count" label="抽取次数" width="100" align="center" sortable />
            <el-table-column prop="start_time" label="开始" width="100" />
            <el-table-column prop="end_time" label="结束" width="100" />
          </el-table>
          <el-pagination style="margin-top:12px;text-align:right" :current-page="page" :page-size="size" :total="total" layout="total,prev,pager,next" @current-change="loadBanners" />
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import * as echarts from 'echarts'
import { getGachaBanners, getGachaStats } from '@/api/analysis/gacha'

export default {
  name: 'GachaAnalysis',
  data() {
    return { kpi:{}, bannerData:[], loading:false, page:1, size:20, total:0 }
  },
  mounted() { this.loadAll() },
  methods: {
    darkRow() { return 'dark-row' },
    async loadAll() {
      const [br, sr] = await Promise.all([
        getGachaBanners({page:1,size:500}),
        getGachaStats()
      ])
      const banners = br.data.rows, stats = sr.data
      // KPI
      const versions = [...new Set(banners.map(x=>x.version_name))]
      let tp=0; const cm={}; banners.forEach(b=>{ tp+=b.pull_count||0; cm[b.char_name]=(cm[b.char_name]||0)+b.pull_count })
      const topCh = Object.entries(cm).sort((a,b)=>b[1]-a[1])[0]
      const verPulls={}; banners.forEach(b=>{ verPulls[b.version_name]=(verPulls[b.version_name]||0)+b.pull_count })
      const topVer = Object.entries(verPulls).sort((a,b)=>b[1]-a[1])[0]
      this.kpi = { periods:versions.length, totalPulls:tp, topChar:topCh?.[0], topPulls:topCh?.[1], topVer:topVer?.[0] }

      // 版本流水图
      const vSorted = Object.entries(verPulls).sort((a,b)=>a[0].localeCompare(b[0]))
      const c1=echarts.init(this.$refs.verChart)
      c1.setOption({ tooltip:{trigger:'axis',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11}},
        grid:{top:10,bottom:30,left:60,right:20},
        xAxis:{type:'category',data:vSorted.map(x=>x[0]),axisLabel:{color:'#8b949e',fontSize:9,rotate:45},axisLine:{lineStyle:{color:'#21262d'}}},
        yAxis:{type:'value',name:'抽取次数',axisLabel:{color:'#8b949e',formatter:v=>(v/10000).toFixed(0)+'万'},splitLine:{lineStyle:{color:'#161b22'}}},
        series:[{type:'bar',data:vSorted.map(x=>x[1]),itemStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:'#58a6ff'},{offset:1,color:'#1a3a5c'}])},barMaxWidth:30}] })

      // TOP10 排行
      const top10=Object.entries(cm).sort((a,b)=>b[1]-a[1]).slice(0,10)
      const c2=echarts.init(this.$refs.topChart)
      c2.setOption({ tooltip:{trigger:'axis',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:11}},
        grid:{top:10,bottom:20,left:70,right:30},
        xAxis:{type:'value',name:'万抽',axisLabel:{color:'#8b949e',formatter:v=>(v/10000).toFixed(0)}},yAxis:{type:'category',data:top10.map(x=>x[0]).reverse(),axisLabel:{color:'#c8d6e5',fontSize:10},axisLine:{lineStyle:{color:'#21262d'}}},
        series:[{type:'bar',data:top10.map(x=>x[1]).reverse(),itemStyle:{color:new echarts.graphic.LinearGradient(0,0,1,0,[{offset:0,color:'#d29922'},{offset:1,color:'#f85149'}])}}] })

      // 类型分布
      const types={}; banners.forEach(b=>{types[b.banner_type||'角色池']=(types[b.banner_type||'角色池']||0)+b.pull_count})
      const c3=echarts.init(this.$refs.typeChart)
      c3.setOption({ tooltip:{trigger:'item',backgroundColor:'rgba(10,20,50,.95)',borderColor:'#30363d',textStyle:{color:'#c8d6e5',fontSize:12}},
        legend:{bottom:0,textStyle:{color:'#8b949e',fontSize:10}},
        series:[{type:'pie',radius:['45%','70%'],center:['50%','48%'],data:Object.entries(types).map(([k,v])=>({name:k,value:v})),label:{color:'#8b949e',formatter:'{b}: {d}%'}}],
        backgroundColor:'transparent' })

      // Table
      this.bannerData = banners.slice(0,20); this.total = banners.length
    },
    async loadBanners() { this.loading=true; const r=await getGachaBanners({page:this.page,size:this.size}); this.bannerData=r.data.rows; this.total=r.data.total; this.loading=false }
  }
}
</script>

<style scoped>
* { margin:0; padding:0; box-sizing:border-box; }
.dark-dashboard { background:#0a0e17; color:#c8d6e5; min-height:100vh; font-family:'Microsoft YaHei','PingFang SC',sans-serif; }
.main { max-width:1500px; margin:0 auto; padding:16px 20px; }
.kpi-row { display:flex; gap:14px; margin-bottom:14px; }
.kpi-card { flex:1; background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:16px 20px; text-align:center; }
.kpi-label { display:block; font-size:12px; color:#8b949e; margin-bottom:6px; }
.kpi-val { display:block; font-size:22px; font-weight:700; color:#e6edf3; }
.kpi-sub { display:block; font-size:11px; color:#8b949e; margin-top:2px; }
.chart-row { display:flex; gap:14px; }
.panel { background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:14px 16px; }
.panel-title { font-size:14px; font-weight:600; color:#e6edf3; margin-bottom:10px; }
::v-deep .dark-th { background:#161b22; color:#8b949e; font-size:12px; border-color:#21262d; }
::v-deep .dark-td { background:#0d1117; color:#c8d6e5; font-size:12px; border-color:#161b22; }
::v-deep .dark-row:hover td { background:#161b22 !important; }
::v-deep .el-pagination button, ::v-deep .el-pager li { background:#0d1117; color:#8b949e; }
::-webkit-scrollbar { width:4px; } ::-webkit-scrollbar-thumb { background:#21262d; border-radius:2px; }
</style>
'''
with open('D:/neu2026bd/projects/ruoyi/ruoyi-ui/src/views/analysis/gacha/index.vue','w',encoding='utf-8') as f:
    f.write(content)
print('done')
