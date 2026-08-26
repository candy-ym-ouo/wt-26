const $ = id => document.getElementById(id);
const colors = ['#7aa2f7','#73daca','#bb9af7','#e0af68','#f7768e','#7dcfff','#9ece6a','#ff9e64'];
let chartSeries = [];

function localValue(date) {
  const adjusted = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return adjusted.toISOString().slice(0, 16);
}

function setupTimes() {
  const now = new Date();
  $('w-start').value = localValue(new Date(now.getTime() - 100000));
  $('q-end').value = localValue(now);
  $('q-start').value = localValue(new Date(now.getTime() - 3600000));
}

function parseTags(raw) {
  const tags = {};
  raw.split(',').map(v => v.trim()).filter(Boolean).forEach(item => {
    const index = item.indexOf('=');
    if (index < 1) throw new Error(`标签格式错误: ${item}`);
    tags[item.slice(0, index)] = item.slice(index + 1);
  });
  return tags;
}

async function api(path, options) {
  const response = await fetch(path, options);
  const body = await response.json();
  if (!response.ok || body.status === 'error') throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}

function notice(message, good = true) {
  const box = $('notice');
  box.textContent = message;
  box.className = good ? 'good' : 'bad';
  clearTimeout(notice.timer);
  notice.timer = setTimeout(() => box.className = '', 4000);
}

document.querySelectorAll('.tab').forEach(button => button.addEventListener('click', () => showView(button.dataset.view)));

function showView(name) {
  document.querySelectorAll('.tab').forEach(tab => tab.classList.toggle('active', tab.dataset.view === name));
  document.querySelectorAll('.view').forEach(view => view.classList.toggle('active', view.id === name));
  if (name === 'metrics') loadMetrics();
  if (name === 'status') loadStatus();
}

$('write-submit').addEventListener('click', async () => {
  try {
    const count = Number($('w-count').value);
    const step = Number($('w-step').value);
    const start = new Date($('w-start').value).getTime();
    const base = Number($('w-base').value);
    const amp = Number($('w-amp').value);
    const points = Array.from({length: count}, (_, index) => ({
      ts: start + index * step,
      value: +(base + amp * Math.sin(index * .1) + (Math.random() - .5) * amp * .2).toFixed(4)
    }));
    const body = {metric: $('w-metric').value.trim(), tags: parseTags($('w-tags').value), points};
    const result = await api('/api/v1/write', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    notice(`成功写入 ${result.written} 个点`);
  } catch (error) { notice(error.message, false); }
});

$('query-submit').addEventListener('click', async () => {
  try {
    const params = new URLSearchParams({
      metric: $('q-metric').value.trim(), tags: $('q-tags').value.trim(),
      start: new Date($('q-start').value).getTime(), end: new Date($('q-end').value).getTime(),
      step: $('q-step').value, agg: $('q-agg').value, fill: $('q-fill').value
    });
    const response = await api(`/api/v1/query_range?${params}`);
    chartSeries = response.data.result;
    drawChart();
    renderTable();
    notice(`查询返回 ${chartSeries.length} 个序列`);
  } catch (error) { notice(error.message, false); }
});

function drawChart() {
  const canvas = $('chart');
  const scale = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = 360;
  canvas.width = width * scale;
  canvas.height = height * scale;
  const ctx = canvas.getContext('2d');
  ctx.scale(scale, scale);
  ctx.clearRect(0, 0, width, height);
  const points = chartSeries.flatMap(series => series.values.map(point => [Number(point[0]), Number(point[1])]));
  if (!points.length) {
    ctx.fillStyle = '#8990b3'; ctx.textAlign = 'center'; ctx.fillText('没有匹配数据', width / 2, height / 2); return;
  }
  const xs = points.map(point => point[0]), ys = points.map(point => point[1]);
  let minX = Math.min(...xs), maxX = Math.max(...xs), minY = Math.min(...ys), maxY = Math.max(...ys);
  if (minX === maxX) maxX += 1;
  if (minY === maxY) { minY -= 1; maxY += 1; }
  const pad = {left:60,right:20,top:20,bottom:35};
  const x = value => pad.left + (value-minX)/(maxX-minX)*(width-pad.left-pad.right);
  const y = value => height-pad.bottom-(value-minY)/(maxY-minY)*(height-pad.top-pad.bottom);
  ctx.strokeStyle = '#3a3a55'; ctx.fillStyle = '#8990b3'; ctx.font = '11px system-ui';
  for (let i=0;i<=5;i++) {
    const py = pad.top+i*(height-pad.top-pad.bottom)/5;
    ctx.beginPath(); ctx.moveTo(pad.left,py); ctx.lineTo(width-pad.right,py); ctx.stroke();
    ctx.fillText((maxY-i*(maxY-minY)/5).toFixed(2), 6, py+4);
  }
  chartSeries.forEach((series,index) => {
    ctx.strokeStyle = colors[index%colors.length]; ctx.lineWidth = 2; ctx.beginPath();
    series.values.forEach((point,i) => { const px=x(Number(point[0])), py=y(Number(point[1])); i ? ctx.lineTo(px,py) : ctx.moveTo(px,py); });
    ctx.stroke();
  });
  $('legend').innerHTML = chartSeries.map((series,index) => `<span class="legend" style="color:${colors[index%colors.length]}">${Object.entries(series.metric).map(([k,v])=>`${k}=${v}`).join(' ')}</span>`).join('');
}

function renderTable() {
  let html = '<table><thead><tr><th>序列</th><th>时间</th><th>值</th></tr></thead><tbody>';
  chartSeries.forEach(series => series.values.slice(0,100).forEach(point => {
    html += `<tr><td>${Object.values(series.metric).join(' / ')}</td><td>${new Date(Number(point[0])).toLocaleString()}</td><td>${point[1]}</td></tr>`;
  }));
  $('data-table').innerHTML = html + '</tbody></table>';
}

async function loadMetrics() {
  try {
    const response = await api('/api/v1/metrics');
    const rows = response.data.map(item => `<tr class="clickable" data-metric="${item.name}"><td>${item.name}</td><td>${item.series}</td><td>${item.points}</td></tr>`).join('');
    $('metrics-table').innerHTML = `<table><thead><tr><th>名称</th><th>序列</th><th>点数</th></tr></thead><tbody>${rows}</tbody></table>`;
    document.querySelectorAll('[data-metric]').forEach(row => row.addEventListener('click', () => { $('q-metric').value=row.dataset.metric; showView('query'); }));
  } catch (error) { notice(error.message, false); }
}

async function loadStatus() {
  try {
    const response = await api('/api/v1/status');
    const value = response.data;
    const cards = [['运行秒数',value.uptime_seconds],['序列',value.series],['写入点数',value.points_written],['内存点数',value.points_in_memory],['查询次数',value.queries_total],['磁盘字节',value.disk_bytes]];
    $('status-cards').innerHTML = cards.map(([name,count]) => `<div class="card">${name}<strong>${count}</strong></div>`).join('');
    $('status-json').textContent = JSON.stringify(value, null, 2);
  } catch (error) { notice(error.message, false); }
}

$('metrics-refresh').addEventListener('click', loadMetrics);
window.addEventListener('resize', drawChart);
setInterval(() => { if ($('status').classList.contains('active')) loadStatus(); }, 5000);
setupTimes();
