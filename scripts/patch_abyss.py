#!/usr/bin/env python3
"""Replace static ABYSS_DATA with API fetch in 历代深渊使用率榜.html"""
import re

path = "D:/neu2026bd/projects/ruoyi/ruoyi-ui/public/analysis/历代深渊使用率榜.html"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Remove _abyss_data.js script tag
content = content.replace('<script src="_abyss_data.js"></script>', '<!-- API loaded via fetch -->')

# 2. Find and replace the old script logic
# Find the script section
script_start = content.find('var DATA = ABYSS_DATA;')
script_end = content.find('</script>', script_start)

if script_start == -1:
    print("ERROR: old script not found")
    exit(1)

new_script = """var ALL_CHARS = [];
var ALL_TEAMS = [];
var sortMode = 'use';

async function loadData() {
  try {
    var charRes = await fetch('/analysis/char/ranking?page=1&size=200&sortBy=use_rate');
    var charJson = await charRes.json();
    ALL_CHARS = charJson.data.rows.map(function(c) {
      return { n: c.char_name, a: c.avatar, st: c.star, or: parseFloat(c.own_rate||0), ur: parseFloat(c.use_rate||0) };
    });
    var teamRes = await fetch('/analysis/team/list');
    var teamJson = await teamRes.json();
    ALL_TEAMS = teamJson.data.map(function(t) {
      var avatars = [];
      try { avatars = JSON.parse(t.avatars_json); } catch(e) {}
      return { name: t.team_name, ur: parseFloat(t.use_rate||0), hr: parseFloat(t.has_rate||0), avatars: avatars };
    });
    document.getElementById('header-info').innerHTML = '<span>\\ud83d\\udcc5 v6.6(第一期)</span><span>\\u7ba1\\u9053\\u4ea7\\u51fa</span>';
    document.getElementById('sample-info').textContent = '\\u89d2\\u8272\\u6570: ' + ALL_CHARS.length;
    document.getElementById('ver-tabs').innerHTML = '<div class=\"ver-tab active\">v6.6</div>';
    renderCurrent();
  } catch(e) {
    document.getElementById('rank-list').innerHTML = '<div style=\"padding:40px;color:#f85149;text-align:center\">\\u6570\\u636e\\u52a0\\u8f7d\\u5931\\u8d25\\uff0c\\u8bf7\\u786e\\u8ba4\\u540e\\u7aef\\u5df2\\u542f\\u52a8</div>';
    console.error(e);
  }
}

function switchVer(idx) {}
function setSortMode(mode) { sortMode = mode; renderCurrent(); }

function renderCurrent() {
  var search = (document.getElementById('search-input').value || '').toLowerCase();
  var chars = ALL_CHARS.filter(function(c) {
    if (search && c.n.toLowerCase().indexOf(search) === -1) return false;
    return true;
  });
  if (sortMode === 'use') chars.sort(function(a,b) { return b.ur - a.ur; });
  else chars.sort(function(a,b) { return b.or - a.or; });

  document.getElementById('rank-header').innerHTML = '\\ud83d\\udcca \\u89d2\\u8272' + (sortMode==='use'?'\\u4f7f\\u7528\\u7387':'\\u6301\\u6709\\u7387') + '\\u6392\\u884c \\u00b7 \\u5171 ' + chars.length + ' \\u540d';
  var listHTML = '';
  chars.forEach(function(c, i) {
    var stars = ''; for (var j=0; j<c.st; j++) stars += '\\u2b50';
    var starCls = c.st === 5 ? 's5' : 's4';
    var topCls = i < 3 ? ' top' : '';
    listHTML += '<div class=\"rank-item\">' +
      '<div class=\"rank-num' + topCls + '\">' + (i+1) + '</div>' +
      (c.a ? '<img class=\"rank-avatar\" src=\"' + c.a + '\" alt=\"\" loading=\"lazy\" onerror=\"this.style.display=\\'none\\'\">' : '') +
      '<div class=\"rank-name\">' + c.n + ' <span class=\"rank-star ' + starCls + '\">' + stars + '</span></div>' +
      '<div class=\"rank-bar-area\">';
    if (sortMode === 'use') {
      listHTML += '<div class=\"rank-rates\"><span class=\"ur\">\\u4f7f\\u7528 ' + c.ur.toFixed(1) + '%</span>' +
        '<span class=\"or\">\\u6301\\u6709 ' + c.or.toFixed(1) + '%</span></div>';
      listHTML += '<div class=\"bar-track\"><div class=\"bar-own\" style=\"width:' + c.or + '%\">' +
        '<div class=\"bar-use\" style=\"width:' + Math.min(c.ur,100) + '%\"></div></div></div>';
    } else {
      listHTML += '<div class=\"rank-rates\"><span class=\"or\">\\u6301\\u6709 ' + c.or.toFixed(1) + '%</span></div>';
      listHTML += '<div class=\"bar-track\"><div class=\"bar-own\" style=\"width:' + c.or + '%\"></div></div>';
    }
    listHTML += '</div></div>';
  });
  document.getElementById('rank-list').innerHTML = listHTML;

  var filteredTeams = ALL_TEAMS;
  if (search) filteredTeams = ALL_TEAMS.filter(function(t) { return t.name.toLowerCase().indexOf(search) !== -1; });
  document.getElementById('team-header').innerHTML = '\\ud83d\\udd17 \\u70ed\\u95e8\\u914d\\u961f \\u00b7 \\u5171 ' + filteredTeams.length + ' \\u652f';
  var teamHTML = '';
  filteredTeams.forEach(function(t, ti) {
    var avatarsHTML = '';
    t.avatars.forEach(function(a) { avatarsHTML += '<img src=\"' + a + '\" alt=\"\" loading=\"lazy\" onerror=\"this.style.display=\\'none\\'\">'; });
    teamHTML += '<div class=\"team-item\"><div class=\"team-num\">#' + (ti+1) + '</div>' +
      '<div class=\"team-avatars\">' + avatarsHTML + '</div>' +
      '<div class=\"team-bar-area\"><div class=\"team-rates\">' +
        '<span class=\"tur\">\\u4f7f\\u7528 ' + t.ur.toFixed(1) + '%</span>' +
        '<span class=\"thr\">\\u6301\\u6709 ' + t.hr.toFixed(1) + '%</span></div>' +
      '<div class=\"team-bar-track\"><div class=\"team-bar-has\" style=\"width:' + t.hr + '%\">' +
        '<div class=\"team-bar-use\" style=\"width:' + Math.min(t.ur,100) + '%\"></div></div></div></div></div>';
  });
  document.getElementById('team-list').innerHTML = teamHTML;
}

loadData();
"""

content = content[:script_start] + new_script + content[script_end:]
with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("Abyss HTML updated successfully")
