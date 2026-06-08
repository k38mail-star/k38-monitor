"""dltrace v2 工业风面板 — HTML/CSS/JS 模板 + 数据层修复"""

INDUSTRIAL_HTML = r"""<!DOCTYPE html><html lang="zh"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dltrace — K38 下载监控</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
<meta http-equiv="refresh" content="5">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#050510;color:#aab;font-family:'JetBrains Mono',monospace;overflow-x:hidden;min-height:100vh}
#app{position:relative;z-index:1;max-width:1320px;margin:0 auto;padding:12px}
.header{text-align:center;padding:8px 0 4px}
.header h1{font-family:'Orbitron',sans-serif;font-size:20px;color:#0ff;letter-spacing:3px;text-shadow:0 0 15px #0ff6}
.header .sub{font-size:9px;color:#334;letter-spacing:1px;margin-top:2px}
.topbar{display:flex;justify-content:space-between;align-items:center;padding:3px 0;font-size:9px;color:#556}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(370px,1fr));gap:8px;margin:6px 0}
.card{background:#08081a;border:1px solid #1a1a3a;border-radius:6px;padding:10px;box-shadow:0 0 12px #0006;position:relative}
.card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;padding-bottom:4px;border-bottom:1px solid #1a1a3a}
.card-title{font-family:'Orbitron',sans-serif;font-size:12px;letter-spacing:1px}
.card-title.online{color:#0f0;text-shadow:0 0 8px #0f04}
.card-title.offline{color:#f44}
.card-ts{font-size:8px;color:#556}
.status-dot{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:4px;vertical-align:middle}
.status-dot.online{background:#0f0;box-shadow:0 0 4px #0f0}
.status-dot.offline{background:#f44;box-shadow:0 0 4px #f44}
.status-dot.done{background:#446}
.stats-row{display:flex;gap:12px;font-size:9px;color:#667;margin-bottom:4px}
.stats-row span b{color:#bbc}
.dl-list{list-style:none}
.dl-item{display:flex;align-items:center;gap:6px;padding:3px 0;border-bottom:1px solid #0a0a1e;font-size:10px}
.dl-item:last-child{border-bottom:none}
.dl-icon{font-size:12px;width:16px;text-align:center}
.dl-info{flex:1;min-width:0}
.dl-name{color:#bbc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dl-bar-wrap{height:4px;background:#0a0a1e;border-radius:2px;margin:2px 0;overflow:hidden}
.dl-bar{height:100%;border-radius:2px;transition:width .5s}
.dl-bar.downloading{background:linear-gradient(90deg,#0f8,#0ff);box-shadow:0 0 4px #0ff6}
.dl-bar.finishing{background:linear-gradient(90deg,#fa0,#ff0);box-shadow:0 0 4px #fa06}
.dl-bar.done{background:#446}
.dl-meta{display:flex;justify-content:space-between;font-size:8px;color:#556}
.dl-meta .pct{font-weight:bold}.dl-meta .pct-ok{color:#0f0}.dl-meta .pct-mid{color:#fa0}.dl-meta .pct-low{color:#0ff}
.dl-meta .speed{color:#667}
.empty-state{text-align:center;padding:12px 0;color:#445;font-size:10px}
.empty-state .icon{font-size:20px;margin-bottom:4px}
.footer{text-align:center;color:#224;font-size:9px;margin-top:10px}
.footer a{color:#448;text-decoration:none}
@media(max-width:600px){.grid{grid-template-columns:1fr}.header h1{font-size:16px}}
</style></head><body><div id="app">
<div class="header"><h1>▣ DLT RACE</h1><div class="sub">DOWNLOAD TRACKER · K38 INDUSTRIAL</div></div>
<div class="topbar"><span>{{STATUS_BAR}}</span><span id="refresh-countdown">5s · auto</span></div>
<div class="grid" id="node-grid">{{NODE_CARDS}}</div>
<div class="footer"><a href="https://github.com/kk38/dltrace" target="_blank">dltrace</a> · {{VERSION}} · {{TS}}</div>
</div>
<script>
// — dltrace v2 工业面板 —
var POLL=3000;
function esc(s){var d=document.createElement('div');d.appendChild(document.createTextNode(s));return d.innerHTML}
function tagIcon(t){var tl=(t||'').toLowerCase();if(tl.match(/model|qwen|llm|safetensors/))return'🧠';if(tl.match(/archive|tar|gz|zip/))return'🗜️';if(tl.match(/git|clone/))return'🔀';if(tl.match(/pip|python/))return'🐍';if(tl.match(/docker|image/))return'🐳';if(tl.match(/video|mp4|movie/))return'🎬';return'📄'}
function pctCls(p){return p>=100?'pct-ok':p>50?'pct-mid':'pct-low'}
function barCls(s){return s==='done'||s==='stale'?'done':s==='finishing'?'finishing':'downloading'}
function buildCard(h,n,d){var f=n.active_files||[],p=n.active_procs||[],dots=n.tracked_total>0||(n.ts||0)>1700000000;
var titleCls=dots?'online':'offline',dotCls=dots?'online':(n.files_count?'done':'offline');
var html='<div class="card"><div class="card-header"><span class="card-title '+titleCls+'"><span class="status-dot '+dotCls+'"></span>'+esc(h)+'</span><span class="card-ts">'+(n.ts_str||'--:--:--')+'</span></div>';
html+='<div class="stats-row"><span>📦 <b>'+n.files_count+'</b></span><span>📊 <b>'+n.tracked_total+'</b> tracked</span>'+(f.length?'<span>⚡ <b>'+f.length+'</b> active</span>':'')+'</div>';
if(f.length){html+='<ul class="dl-list">';
for(var i=0;i<f.length;i++){var ff=f[i];var p=ff.pct||0;var sz=ff.size_mb||0;var sp=ff.speed_mb||0;
var nm=ff.name||'';var st=ff.status||'';var ic=tagIcon(ff.tag);var bc=barCls(st);
html+='<li class="dl-item"><span class="dl-icon">'+ic+'</span><div class="dl-info"><div class="dl-name" title="'+esc(nm)+'">'+esc(nm)+'</div><div class="dl-bar-wrap"><div class="dl-bar '+bc+'" style="width:'+p+'%"></div></div><div class="dl-meta"><span class="pct '+pctCls(p)+'">'+p+'%</span><span style="color:#556">'+sz.toFixed(1)+' MB</span><span class="speed">'+(sp>0?sp.toFixed(2)+' MB/s':'')+'</span><span style="color:#445">'+st+'</span></div></div></li>';}
html+='</ul>'}else{html+='<div class="empty-state"><div class="icon">📭</div><p>无活跃下载</p></div>';}
html+='</div>';return html}
function render(d){var ns=d.nodes||{};var keys=Object.keys(ns);var grid=document.getElementById('node-grid');
var c='';for(var i=0;i<keys.length;i++){var h=keys[i];c+=buildCard(h,ns[h],d);}
grid.innerHTML=c;var sb='';var total=0;for(var k in ns){total+=ns[k].tracked_total||0;}
sb='<span class="status-dot online"></span> '+keys.length+' nodes · '+total+' files';
document.querySelector('.topbar span:first-child').innerHTML=sb;
var meta=document.querySelector('.footer');if(meta){meta.innerHTML=meta.innerHTML.replace(/\d+\.\d+\.\d+/,(d.version||'0.1.0'));}}
// 初始渲染
try{render({{DATA}});}catch(e){}
// 停meta refresh
var mr=document.querySelector('meta[http-equiv="refresh"]');if(mr)mr.parentNode.removeChild(mr);
(function poll(){setTimeout(function(){fetch('/api/v1/metrics').then(function(r){return r.json()}).then(function(d){render(d);poll();}).catch(function(){setTimeout(poll,POLL);});},POLL);})();
</script></body></html>"""
