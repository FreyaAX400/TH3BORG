function fmtUptime(s) {
  if (!s) return '??';
  const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600),
        m = Math.floor((s % 3600) / 60), sec = s % 60;
  return d > 0 ? `${d}d ${h}h ${m}m` : h > 0 ? `${h}h ${m}m ${sec}s` : m > 0 ? `${m}m ${sec}s` : `${sec}s`;
}

function hexIcon(cls) {
  const colors = { up: '#3aff8c', down: '#ff3a3a', unknown: '#ffb83a' };
  const c = colors[cls] || colors.unknown;
  const glow = cls === 'down'
    ? `filter: drop-shadow(0 0 3px ${c}); animation: blink 0.8s step-end infinite;`
    : `filter: drop-shadow(0 0 3px ${c});`;
  return `<svg viewBox="0 0 10 12" class="hex-status"><polygon points="5,0 10,3 10,9 5,12 0,9 0,3" fill="${c}" style="${glow}"/></svg>`;
}

function statBar(val, cls) {
  return `<div class="stat-bar"><div class="stat-fill ${cls || ''}" style="width:${val ?? 0}%"></div></div>`;
}

function barClass(val, warnAt, critAt) {
  warnAt = warnAt || 60; critAt = critAt || 80;
  return val >= critAt ? 'critical' : val >= warnAt ? 'warning' : '';
}

function updateClock() {
  document.getElementById('sys-time').textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
}
setInterval(updateClock, 1000);
updateClock();

async function refresh() {
  try {
    const data = await fetch('/api/status').then(r => r.json());

    // SERVICES
    const allUp = data.services.every(s => s.status === 'up');
    const anyDown = data.services.some(s => s.status === 'down');

    document.getElementById('service-list').innerHTML = data.services.map(s => `
      <a class="service-item" href="${s.url}" target="_blank" rel="noopener">
        <div class="service-hex">${hexIcon(s.status)}</div>
        <span class="service-name">${s.name}</span>
        <div class="service-meta">
          <span class="service-url">${s.url.replace('https://', '')}</span>
          ${s.ping ? `<span class="service-ping">${s.ping}ms</span>` : ''}
        </div>
      </a>`).join('');

    document.getElementById('service-count').textContent =
      `${data.services.filter(s => s.status === 'up').length}/${data.services.length}`;

    const badge = document.getElementById('cluster-badge');
    if (allUp) {
      badge.className = 'cluster-badge nominal';
      badge.innerHTML = '<div class="badge-dot"></div>NOMINAL';
    } else if (anyDown) {
      badge.className = 'cluster-badge degraded';
      badge.innerHTML = '<div class="badge-dot"></div>DEGRADED';
    } else {
      badge.className = 'cluster-badge partial';
      badge.innerHTML = '<div class="badge-dot"></div>PARTIAL';
    }

    // CLUSTER STATS
    const c = data.cluster;
    document.getElementById('stats-content').innerHTML = `
      <div class="stat-block">
        <div class="stat-header"><span class="stat-name">CPU LOAD</span><span class="stat-val">${c.cpu_pct ?? '??'}%</span></div>
        ${statBar(c.cpu_pct, barClass(c.cpu_pct))}
      </div>
      <div class="stat-block">
        <div class="stat-header"><span class="stat-name">MEMORY</span><span class="stat-val">${c.mem_used_pct ?? '??'}%</span></div>
        ${statBar(c.mem_used_pct, barClass(c.mem_used_pct, 70, 85))}
      </div>
      <div class="stat-block">
        <div class="stat-header"><span class="stat-name">DISK</span><span class="stat-val">${c.disk_used_pct ?? '??'}%</span></div>
        ${statBar(c.disk_used_pct, barClass(c.disk_used_pct, 70, 85))}
      </div>
      <div class="cluster-grid">
        <div class="cluster-stat"><div class="cluster-stat-label">TOTAL RAM</div><div class="cluster-stat-val">${c.mem_total_gb ?? '??'}<span>GB</span></div></div>
        <div class="cluster-stat"><div class="cluster-stat-label">FREE RAM</div><div class="cluster-stat-val">${c.mem_avail_gb ?? '??'}<span>GB</span></div></div>
        <div class="cluster-stat"><div class="cluster-stat-label">PODS</div><div class="cluster-stat-val">${c.pods ?? '??'}<span> ACTIVE</span></div></div>
        <div class="cluster-stat"><div class="cluster-stat-label">NODES</div><div class="cluster-stat-val">${c.node_count ?? '??'}<span> ONLINE</span></div></div>
        <div class="cluster-stat" style="grid-column:1/3"><div class="cluster-stat-label">UPTIME</div><div class="cluster-stat-val">${fmtUptime(c.uptime_seconds)}</div></div>
      </div>`;

    // NODES
    if (data.nodes && data.nodes.length) {
      document.getElementById('nodes-content').innerHTML = data.nodes.map(n => `
        <div class="node-card">
          <div class="node-name">${n.name.toUpperCase()}</div>
          <div class="node-meta">${n.pods ?? '?'} PODS &nbsp;·&nbsp; UP ${fmtUptime(n.uptime_seconds)}</div>
          <div class="stat-block">
            <div class="stat-header"><span class="stat-name">CPU</span><span class="stat-val">${n.cpu_pct ?? '??'}%</span></div>
            ${statBar(n.cpu_pct, barClass(n.cpu_pct))}
          </div>
          <div class="stat-block">
            <div class="stat-header"><span class="stat-name">MEM</span><span class="stat-val">${n.mem_used_pct ?? '??'}% · ${n.mem_avail_gb ?? '??'}GB free</span></div>
            ${statBar(n.mem_used_pct, barClass(n.mem_used_pct, 70, 85))}
          </div>
          <div class="stat-block">
            <div class="stat-header"><span class="stat-name">DISK</span><span class="stat-val">${n.disk_used_pct ?? '??'}% · ${n.disk_avail_gb ?? '??'}GB free</span></div>
            ${statBar(n.disk_used_pct, barClass(n.disk_used_pct, 70, 85))}
          </div>
        </div>`).join('');
    }

    // ERRORS
    const el = document.getElementById('error-list');
    if (!data.errors || !data.errors.length) {
      el.innerHTML = '<div class="no-errors">' + hexIcon('up') + '&nbsp;NO ISSUES DETECTED</div>';
    } else {
      el.innerHTML = data.errors.map(i => `
        <div class="error-item ${i.level === 'warning' ? 'warning' : ''}">
          <div class="error-title">${i.title}</div>
          <div class="error-meta">${i.project} · ${i.count} occurrences · ${new Date(i.lastSeen).toLocaleString()}</div>
        </div>`).join('');
    }

    document.getElementById('last-refresh').textContent =
      'UPDATED ' + new Date().toLocaleTimeString('en-GB', { hour12: false });

  } catch (e) {
    console.error('Status fetch failed:', e);
  }
}

refresh();
setInterval(refresh, 30000);