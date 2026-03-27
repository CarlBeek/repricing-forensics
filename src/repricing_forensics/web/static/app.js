/* EIP-7904 Analysis — Client-side rendering */

const COLORS = {
  broken: '#e74c3c',
  changed: '#e67e22',
  saved: '#27ae60',
  neutral: '#95a5a6',
  call_tree: '#3498db',
  event_logs: '#f39c12',
};
const PIE_COLORS = ['#e74c3c', '#3498db', '#27ae60', '#f39c12', '#8e44ad', '#1abc9c'];
const SANKEY_PALETTE = ['#3498db', '#e67e22', '#27ae60', '#8e44ad', '#e74c3c'];

const LAYOUT_DEFAULTS = {
  template: 'plotly_white',
  font: { size: 13 },
  margin: { l: 80, r: 20, t: 50, b: 70 },
};

// ── Helpers ──────────────────────────────────────────────────────────

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

function fmtCount(n) {
  n = Number(n);
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e4) return (n / 1e3).toFixed(1) + 'K';
  return n.toLocaleString();
}

function fmtGas(n) {
  n = Number(n);
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(1) + 'T';
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return Math.round(n).toString();
}

function fmtPct(n) {
  if (Math.abs(n) < 0.01) return n.toFixed(4) + '%';
  if (Math.abs(n) < 0.1) return n.toFixed(3) + '%';
  if (Math.abs(n) < 1) return n.toFixed(2) + '%';
  return n.toFixed(1) + '%';
}

function layout(overrides) {
  const merged = Object.assign({}, LAYOUT_DEFAULTS, overrides);
  // Only touch axes that were explicitly provided — don't create them
  // (creating them on pie/sankey charts causes spurious axis rendering)
  for (const key of ['xaxis', 'yaxis']) {
    if (merged[key]) {
      merged[key].automargin = true;
      // Convert string title to object with standoff
      if (typeof merged[key].title === 'string') {
        merged[key].title = { text: merged[key].title, standoff: 20 };
      } else if (merged[key].title && typeof merged[key].title === 'object' && !merged[key].title.standoff) {
        merged[key].title.standoff = 20;
      }
    }
  }
  return merged;
}

// ── Poster cards ─────────────────────────────────────────────────────

function renderPosterCards(containerId, cards) {
  const el = document.getElementById(containerId);
  el.innerHTML = cards.map(c => `
    <div class="poster-card" ${c.color ? `style="border-top-color:${c.color}"` : ''}>
      <div class="number" ${c.color ? `style="color:${c.color}"` : ''}>${c.value}</div>
      <div class="label">${c.label}</div>
    </div>
  `).join('');
}

// ── Chart renderers ──────────────────────────────────────────────────

function renderBar(divId, data, xKey, yKey, opts = {}) {
  Plotly.newPlot(divId, [{
    x: data.map(r => r[xKey]),
    y: data.map(r => r[yKey]),
    type: 'bar',
    marker: { color: opts.color || COLORS.broken },
    orientation: opts.horizontal ? 'h' : undefined,
  }], layout({
    title: { text: opts.title || '' },
    xaxis: Object.assign({ title: opts.xTitle || '', tickangle: opts.tickAngle || 0 }, opts.xaxis || {}),
    yaxis: Object.assign({ title: opts.yTitle || '', autorange: opts.horizontal ? 'reversed' : undefined }, opts.yaxis || {}),
    height: opts.height || 400,
    margin: opts.margin || LAYOUT_DEFAULTS.margin,
  }), { responsive: true });
}

function renderPie(divId, labels, values, opts = {}) {
  Plotly.newPlot(divId, [{
    labels: labels,
    values: values,
    type: 'pie',
    hole: opts.hole || 0.4,
    textinfo: 'label+percent',
    marker: { colors: PIE_COLORS.slice(0, labels.length) },
  }], layout({
    title: { text: opts.title || '' },
    height: opts.height || 400,
  }), { responsive: true });
}

function renderScatter(divId, data, xKey, yKey, opts = {}) {
  const traces = [{
    x: data.map(r => r[xKey]),
    y: data.map(r => r[yKey]),
    mode: opts.mode || 'lines+markers',
    marker: { size: 5, color: opts.color || COLORS.broken },
    line: { color: opts.color || COLORS.broken, width: 2 },
    fill: opts.fill || undefined,
    fillcolor: opts.fillcolor || undefined,
    name: opts.name || '',
  }];
  const shapes = (opts.hlines || []).map(h => ({
    type: 'line', x0: 0, x1: 1, xref: 'paper', y0: h.y, y1: h.y,
    line: { dash: 'dash', color: '#95a5a6' },
  }));
  Plotly.newPlot(divId, traces, layout({
    title: { text: opts.title || '' },
    xaxis: { title: opts.xTitle || '' },
    yaxis: { title: opts.yTitle || '' },
    height: opts.height || 400,
    shapes: shapes,
  }), { responsive: true });
}

function renderSankey(divId, data, opts = {}) {
  const nodeColors = data.labels.map((_, i) => SANKEY_PALETTE[i % SANKEY_PALETTE.length]);
  Plotly.newPlot(divId, [{
    type: 'sankey',
    node: { label: data.labels, color: nodeColors, pad: 20, thickness: 25 },
    link: {
      source: data.sources,
      target: data.targets,
      value: data.values,
      color: data.link_colors || data.values.map(() => 'rgba(52,152,219,0.3)'),
    },
  }], layout({
    title: { text: opts.title || '' },
    height: opts.height || 550,
    width: opts.width || undefined,
  }), { responsive: true });
}

// ── Table sorting ────────────────────────────────────────────────────

const _sortDir = {};
function sortTable(tableId, col) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const dir = _sortDir[tableId + col] = !(_sortDir[tableId + col] || false);

  rows.sort((a, b) => {
    let aVal = a.cells[col].getAttribute('data-sort') || a.cells[col].textContent.trim();
    let bVal = b.cells[col].getAttribute('data-sort') || b.cells[col].textContent.trim();
    const aNum = parseFloat(aVal.replace(/[^0-9.\-]/g, ''));
    const bNum = parseFloat(bVal.replace(/[^0-9.\-]/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return dir ? bNum - aNum : aNum - bNum;
    }
    return dir ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal);
  });

  rows.forEach(row => tbody.appendChild(row));
}

// ── Search with autocomplete ─────────────────────────────────────────

function initSearch(inputId, listId, onSelect) {
  const input = document.getElementById(inputId);
  const list = document.getElementById(listId);
  let debounce = null;

  input.addEventListener('input', () => {
    clearTimeout(debounce);
    const q = input.value.trim();
    if (q.length < 2) { list.innerHTML = ''; return; }
    debounce = setTimeout(async () => {
      const results = await fetchJSON(`/api/search?q=${encodeURIComponent(q)}`);
      list.innerHTML = results.map(r => `
        <div class="autocomplete-item" data-addr="${r.recipient}">
          <span class="count">${fmtCount(r.broken_txs)} broken</span>
          <span class="name">${escHtml(r.name)}</span><br>
          <span class="addr">${r.recipient}</span>
        </div>
      `).join('');
      list.querySelectorAll('.autocomplete-item').forEach(item => {
        item.addEventListener('click', () => {
          list.innerHTML = '';
          input.value = item.getAttribute('data-addr');
          onSelect(item.getAttribute('data-addr'));
        });
      });
    }, 200);
  });

  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-wrapper')) list.innerHTML = '';
  });
}

function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
