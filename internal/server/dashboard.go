package server

import "net/http"

// Static operator dashboard. Vanilla HTML+CSS+JS, no framework, no external
// resources. Token persists to localStorage so reload doesn't re-prompt.
//
// Why a separate file: even at ~3 KB of HTML, embedding it inline in http.go
// makes the handler unreadable. One Go string, one route, one purpose.
//
// Security model: the page itself is unauthenticated (it's static HTML, just
// rendering chrome). All data fetches go to /admin/stats which IS authenticated.
// Pasting the admin token into a browser tab is no worse than running curl.
const dashboardHTML = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>cosift dashboard</title>
<style>
  body { font: 14px/1.5 -apple-system, system-ui, sans-serif; max-width: 960px; margin: 2em auto; padding: 0 1em; color: #222; }
  h1 { font-size: 1.4em; margin: 0 0 .5em; }
  .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1em; }
  .card { border: 1px solid #ddd; border-radius: 6px; padding: 1em; background: #fafafa; }
  .card h2 { font-size: .75em; font-weight: 600; text-transform: uppercase; color: #888; margin: 0 0 .3em; letter-spacing: .05em; }
  .card .v { font-size: 1.6em; font-weight: 600; font-variant-numeric: tabular-nums; }
  table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
  th, td { text-align: left; padding: .35em .6em; border-bottom: 1px solid #eee; }
  th { color: #888; font-weight: 600; font-size: .85em; text-transform: uppercase; letter-spacing: .03em; }
  .toolbar { display: flex; gap: .5em; align-items: center; margin-bottom: 1em; }
  .toolbar input { flex: 1; padding: .4em .6em; border: 1px solid #ccc; border-radius: 4px; font-family: inherit; }
  .toolbar button { padding: .4em 1em; border: none; border-radius: 4px; background: #2563eb; color: #fff; cursor: pointer; }
  .toolbar button:hover { background: #1d4ed8; }
  .err { color: #b91c1c; margin: .5em 0; }
  .pill { display: inline-block; padding: .1em .5em; border-radius: 999px; font-size: .8em; }
  .pill.on { background: #dcfce7; color: #166534; }
  .pill.off { background: #fee2e2; color: #991b1b; }
  .footnote { color: #888; font-size: .85em; margin-top: 2em; }
</style>
</head><body>
<h1>cosift</h1>
<div class="toolbar">
  <input id="token" type="password" placeholder="admin token (Authorization: Bearer ...)" />
  <button onclick="refresh()">refresh</button>
</div>
<div id="err" class="err"></div>
<div id="content"></div>
<div class="footnote">auto-refreshes every 30s · token saved to localStorage · <code>GET /admin/stats</code></div>
<script>
  const $ = (id) => document.getElementById(id);
  const tokenInput = $('token');
  tokenInput.value = localStorage.getItem('cosift_token') || '';
  tokenInput.addEventListener('change', () => localStorage.setItem('cosift_token', tokenInput.value));

  function pill(label, on) {
    return '<span class="pill ' + (on ? 'on' : 'off') + '">' + label + (on ? ' on' : ' off') + '</span>';
  }

  function render(d) {
    let html = '<div class="row">';
    html += '<div class="card"><h2>documents</h2><div class="v">' + d.documents.toLocaleString() + '</div></div>';
    html += '<div class="card"><h2>terms</h2><div class="v">' + d.terms.toLocaleString() + '</div></div>';
    html += '<div class="card"><h2>passages</h2><div class="v">' + d.passages.toLocaleString() + '</div></div>';
    // Show as both
    // raw count and percent — the percent is what operators actually need to
    // gauge "how much of my corpus is date-filterable."
    if (typeof d.docs_with_published_at === 'number' && d.documents > 0) {
      const pct = (100 * d.docs_with_published_at / d.documents).toFixed(0);
      html += '<div class="card"><h2>with publish date</h2><div class="v">' +
        d.docs_with_published_at.toLocaleString() + ' <span style="font-size:0.6em;color:#888">(' + pct + '%)</span></div></div>';
    }
    html += '<div class="card"><h2>frontier · queued</h2><div class="v">' + d.frontier.Queued.toLocaleString() + '</div></div>';
    html += '<div class="card"><h2>frontier · in_flight</h2><div class="v">' + d.frontier.InFlight.toLocaleString() + '</div></div>';
    html += '<div class="card"><h2>frontier · done</h2><div class="v">' + d.frontier.Done.toLocaleString() + '</div></div>';
    html += '<div class="card"><h2>frontier · errored</h2><div class="v">' + d.frontier.Errored.toLocaleString() + '</div></div>';
    // Both cards hidden when
    // count is 0 — fresh deployments with no LLM activity shouldn't see
    // empty cells. Matches the CLI's "=== LLM caches ===" hide-when-empty
    // behavior.
    if (d.paraphrases > 0) {
      html += '<div class="card"><h2>paraphrases</h2><div class="v">' + d.paraphrases.toLocaleString() + '</div></div>';
    }
    if (d.hyde_cache > 0) {
      html += '<div class="card"><h2>hyde cache</h2><div class="v">' + d.hyde_cache.toLocaleString() + '</div></div>';
    }
    html += '</div>';

    html += '<h2 style="margin-top:2em">capabilities</h2><p>';
    html += pill('dense', d.dense_enabled) + ' ';
    html += pill('answer', d.answer_enabled) + ' ';
    html += pill('on-demand fetch', d.fetcher_enabled) + '</p>';
    if (d.embedder) html += '<p>embedder: <code>' + d.embedder + '</code></p>';
    if (d.chat) html += '<p>chat: <code>' + d.chat + '</code></p>';
    if (d.reranker) html += '<p>reranker: <code>' + d.reranker + '</code></p>';

    if (d.top_domains && Object.keys(d.top_domains).length) {
      html += '<h2 style="margin-top:2em">top domains</h2><table><tr><th>domain</th><th style="text-align:right">docs</th></tr>';
      Object.entries(d.top_domains).sort((a,b) => b[1] - a[1]).forEach(([k, v]) => {
        html += '<tr><td>' + (k || '(none)') + '</td><td style="text-align:right">' + v.toLocaleString() + '</td></tr>';
      });
      html += '</table>';
    }
    $('content').innerHTML = html;
  }

  async function refresh() {
    $('err').textContent = '';
    const token = tokenInput.value.trim();
    if (!token) { $('err').textContent = 'enter admin token first'; return; }
    try {
      const resp = await fetch('/admin/stats', { headers: { 'Authorization': 'Bearer ' + token } });
      if (resp.status === 401) { $('err').textContent = '401 — wrong token'; return; }
      if (resp.status === 403) { $('err').textContent = '403 — admin endpoints disabled (server has no admin_token configured)'; return; }
      if (!resp.ok) { $('err').textContent = 'http ' + resp.status; return; }
      render(await resp.json());
    } catch (e) {
      $('err').textContent = 'fetch failed: ' + e.message;
    }
  }
  refresh();
  setInterval(refresh, 30000);
</script>
</body></html>`

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}
