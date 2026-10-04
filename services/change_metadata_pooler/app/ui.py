"""Minimal HTML UI for registry, runs, and per-table pool history."""

from __future__ import annotations

import html
import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote


def _esc(v: Any) -> str:
    if v is None:
        return ""
    return html.escape(str(v))


def _nav(active: str) -> str:
    def link(href: str, label: str, key: str) -> str:
        cls = "navlink active" if active == key else "navlink"
        return f'<a class="{cls}" href="{href}">{_esc(label)}</a>'

    return (
        '<nav class="nav">'
        f'{link("/", "Tables", "tables")}'
        f'{link("/ui/runs", "Runs", "runs")}'
        f'{link("/ui/register", "Register", "register")}'
        f'{link("/ui/permissions", "Permissions", "permissions")}'
        f'{link("/v1/tables", "JSON", "json")}'
        "</nav>"
    )


def _layout(
    title: str,
    body: str,
    *,
    user_email: Optional[str] = None,
    active: str = "tables",
    oauth_client_id: str = "",
    extra_head: str = "",
) -> str:
    who = _esc(user_email) if user_email else ""
    oauth_meta = (
        f'<meta name="oauth-client-id" content="{_esc(oauth_client_id)}"/>'
        if oauth_client_id
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <meta name="color-scheme" content="light dark"/>
  {oauth_meta}
  <title>{_esc(title)}</title>
  {extra_head}
  <style>
    :root {{
      --bg: #f4f2ec;
      --ink: #141414;
      --muted: #4a4a4a;
      --line: #cfc9bb;
      --card: #ffffff;
      --header-bg: #1e2430;
      --header-ink: #f4f6fa;
      --header-muted: #c5ccd8;
      --accent: #3d8bfd;
      --ok: #0a7a3e;
      --warn: #9a6700;
      --bad: #b42318;
      --info: #175cd3;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #12151c;
        --ink: #eef1f6;
        --muted: #a7b0c0;
        --line: #2c3340;
        --card: #1a1f29;
        --header-bg: #0d1016;
        --header-ink: #f4f6fa;
        --header-muted: #b7c0d0;
        --accent: #6ea8fe;
        --ok: #3dd68c;
        --warn: #f5c518;
        --bad: #ff6b6b;
        --info: #74b0ff;
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: var(--bg); color: var(--ink); line-height: 1.45;
    }}
    header.appbar {{
      padding: 1rem 1.5rem;
      border-bottom: 1px solid #000;
      background: var(--header-bg);
      color: var(--header-ink);
      display: flex; justify-content: space-between; gap: 1rem; align-items: center;
      flex-wrap: wrap;
    }}
    header.appbar h1 {{
      margin: 0; font-size: 1.2rem; font-weight: 700; letter-spacing: -0.02em;
      color: var(--header-ink);
    }}
    header.appbar a {{ color: var(--accent); text-decoration: none; }}
    header.appbar .meta {{ color: var(--header-muted); font-size: 0.85rem; }}
    header.appbar .titleblock .meta {{ margin-top: 0.2rem; }}
    .nav {{ display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap; }}
    .navlink {{
      color: var(--header-muted); text-decoration: none; font-size: 0.9rem;
      padding: 0.25rem 0.55rem; border-radius: 6px; border: 1px solid transparent;
    }}
    .navlink:hover, .navlink.active {{
      color: var(--header-ink); border-color: #556070; background: #2a3140;
    }}
    main {{ padding: 1.25rem 1.5rem 3rem; max-width: 1200px; }}
    .toolbar {{
      display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;
      margin-bottom: 1rem;
    }}
    .toolbar label {{ color: var(--muted); font-size: 0.9rem; }}
    input[type="search"] {{
      border: 1px solid var(--line); border-radius: 6px; padding: 0.45rem 0.65rem;
      min-width: 16rem; background: var(--card); color: var(--ink);
    }}
    table {{
      width: 100%; border-collapse: collapse; background: var(--card);
      border: 1px solid var(--line); border-radius: 8px; overflow: hidden;
    }}
    th, td {{
      text-align: left; padding: 0.55rem 0.7rem; border-bottom: 1px solid var(--line);
      font-size: 0.9rem; vertical-align: top;
    }}
    th {{ background: color-mix(in srgb, var(--card) 70%, var(--line)); font-weight: 600; position: sticky; top: 0; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ filter: brightness(1.05); }}
    a.rowlink {{ color: var(--accent); text-decoration: none; font-weight: 500; }}
    a.rowlink:hover {{ text-decoration: underline; }}
    .pill {{
      display: inline-block; padding: 0.1rem 0.45rem; border-radius: 999px;
      font-size: 0.75rem; font-weight: 600; border: 1px solid var(--line);
      background: var(--card); color: var(--ink);
    }}
    .pill.ok {{ color: var(--ok); border-color: color-mix(in srgb, var(--ok) 40%, var(--line)); }}
    .pill.error, .pill.bad {{ color: var(--bad); }}
    .pill.warn, .pill.warning {{ color: var(--warn); }}
    .pill.info {{ color: var(--info); }}
    .muted {{ color: var(--muted); }}
    .mono {{ font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 0.82rem; }}
    .panel {{
      background: var(--card); border: 1px solid var(--line); border-radius: 8px;
      padding: 1rem 1.1rem; margin-bottom: 1rem;
    }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 0.75rem; }}
    .kv .k {{ color: var(--muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    .kv .v {{ font-weight: 560; margin-top: 0.15rem; word-break: break-all; }}
    /* Pool-history: fixed cols so long Error URLs don't crush Partition ids */
    table.table-logs {{ table-layout: fixed; }}
    table.table-logs th, table.table-logs td {{ overflow: hidden; }}
    table.table-logs .col-ids {{ width: 22%; }}
    table.table-logs .col-err {{ width: 28%; }}
    table.table-logs .col-delta {{ width: 14%; }}
    table.table-logs .col-pooled {{ width: 12%; }}
    pre.ids, pre.logline, pre.err {{
      white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; margin: 0;
      font-size: 0.78rem; max-height: 6rem; overflow: auto;
    }}
    .log {{
      background: var(--card); border: 1px solid var(--line); border-radius: 8px;
      font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 0.82rem;
    }}
    .log .line {{
      display: grid; grid-template-columns: 11rem 5.5rem 1fr;
      gap: 0.75rem; padding: 0.45rem 0.75rem; border-bottom: 1px solid var(--line);
    }}
    .log .line:last-child {{ border-bottom: none; }}
    .lvl-INFO {{ color: var(--info); font-weight: 700; }}
    .lvl-WARNING {{ color: var(--warn); font-weight: 700; }}
    .lvl-ERROR {{ color: var(--bad); font-weight: 700; }}
    .btn {{
      display: inline-block; border: 1px solid var(--line); background: var(--card);
      color: var(--ink); border-radius: 6px; padding: 0.4rem 0.75rem; cursor: pointer;
      font-size: 0.85rem; text-decoration: none;
    }}
    .btn.primary {{ background: var(--accent); border-color: var(--accent); color: #fff; }}
    .btn.danger {{
      color: var(--bad); border-color: color-mix(in srgb, var(--bad) 45%, var(--line));
    }}
    .btn.danger:hover {{ background: color-mix(in srgb, var(--bad) 12%, var(--card)); }}
    .btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    input[type="text"], select {{
      border: 1px solid var(--line); border-radius: 6px; padding: 0.45rem 0.65rem;
      background: var(--card); color: var(--ink); min-width: 22rem;
    }}
    .stack {{ display: flex; flex-direction: column; gap: 0.75rem; }}
    .msg {{ padding: 0.65rem 0.8rem; border-radius: 6px; border: 1px solid var(--line); }}
    .msg.err {{ border-color: var(--bad); color: var(--bad); }}
    .msg.ok {{ border-color: var(--ok); color: var(--ok); }}
    pre.cmd {{
      background: var(--card); border: 1px solid var(--line); border-radius: 6px;
      padding: 0.65rem 0.8rem; overflow: auto; font-size: 0.78rem;
    }}
    .table-pick {{ max-height: 16rem; overflow: auto; border: 1px solid var(--line);
      border-radius: 6px; padding: 0.5rem; background: var(--card); }}
    dialog.confirm {{
      border: 1px solid var(--line); border-radius: 10px; background: var(--card);
      color: var(--ink); padding: 1.1rem 1.2rem; max-width: 28rem; width: calc(100% - 2rem);
      box-shadow: 0 12px 40px rgba(0,0,0,0.25);
    }}
    dialog.confirm::backdrop {{ background: rgba(0,0,0,0.45); }}
    dialog.confirm .actions {{ display:flex; gap:0.5rem; justify-content:flex-end; margin-top:1rem; }}
  </style>
</head>
<body>
  <header class="appbar">
    <div class="titleblock">
      <h1><a href="/">change-metadata-pooler</a></h1>
      <div class="meta">{_esc(title)}</div>
    </div>
    <div style="display:flex;gap:1rem;align-items:center;flex-wrap:wrap;">
      {_nav(active)}
      <div class="meta">{who}</div>
    </div>
  </header>
  <main>
    {body}
  </main>
</body>
</html>
"""

_GRANT_JS = """
<script src="https://accounts.google.com/gsi/client" async defer></script>
<script>
// GIS only works with a *Web* OAuth client that lists this Cloud Run URL under
// Authorized JavaScript origins. IAP clients return invalid_client / no registered origin.
function oauthClientId() {
  return document.querySelector('meta[name="oauth-client-id"]')?.content || '';
}

function ensureTokenPanel() {
  let el = document.getElementById('tokenPanel');
  if (el) return el;
  el = document.createElement('div');
  el.id = 'tokenPanel';
  el.className = 'panel stack';
  el.style.display = 'none';
  el.innerHTML = `
    <div><b>Grant as your user</b></div>
    <div class="muted">IAP login is not a GCP access token. Paste a short-lived token from a terminal
      where you are logged in as an owner/editor of the dataset or project:</div>
    <pre class="cmd">gcloud auth print-access-token</pre>
    <label>Access token
      <div><input type="password" id="accessTokenInput" autocomplete="off"
        placeholder="ya29...." style="min-width:100%; width:100%;"/></div>
    </label>
    <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
      <button class="btn primary" type="button" id="tokenGrantBtn">Grant with token</button>
      <button class="btn" type="button" id="tokenCancelBtn">Cancel</button>
      <button class="btn" type="button" id="gisGrantBtn" style="display:none;">Try Google popup</button>
    </div>
    <div id="tokenPanelStatus" class="muted"></div>
  `;
  document.querySelector('main')?.prepend(el);
  return el;
}

function requestGisToken(clientId) {
  return new Promise((resolve, reject) => {
    if (!window.google?.accounts?.oauth2) {
      reject(new Error('Google Identity Services not loaded'));
      return;
    }
    const tokenClient = google.accounts.oauth2.initTokenClient({
      client_id: clientId,
      scope: 'https://www.googleapis.com/auth/cloud-platform',
      callback: (resp) => {
        if (resp.error) reject(new Error(resp.error));
        else resolve(resp.access_token);
      },
      error_callback: (err) => reject(new Error(err?.message || 'GIS error')),
    });
    tokenClient.requestAccessToken({ prompt: 'consent' });
  });
}

async function getAccessTokenInteractive() {
  const clientId = oauthClientId();
  // Prefer paste UI — reliable for owners; GIS only if a real Web client is configured.
  const panel = ensureTokenPanel();
  panel.style.display = 'block';
  const input = document.getElementById('accessTokenInput');
  const status = document.getElementById('tokenPanelStatus');
  const gisBtn = document.getElementById('gisGrantBtn');
  if (clientId) gisBtn.style.display = 'inline-block';
  status.textContent = '';
  input.focus();

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      document.getElementById('tokenGrantBtn').onclick = null;
      document.getElementById('tokenCancelBtn').onclick = null;
      document.getElementById('gisGrantBtn').onclick = null;
    };
    document.getElementById('tokenGrantBtn').onclick = () => {
      const t = (input.value || '').trim();
      if (!t) { status.textContent = 'Paste a token first.'; return; }
      cleanup();
      panel.style.display = 'none';
      resolve(t);
    };
    document.getElementById('tokenCancelBtn').onclick = () => {
      cleanup();
      panel.style.display = 'none';
      reject(new Error('Grant cancelled'));
    };
    document.getElementById('gisGrantBtn').onclick = async () => {
      status.textContent = 'Opening Google…';
      try {
        const t = await requestGisToken(clientId);
        cleanup();
        panel.style.display = 'none';
        resolve(t);
      } catch (e) {
        status.textContent = 'Google popup failed (' + (e.message || e) +
          '). Use a Web OAuth client with this origin registered, or paste a token.';
      }
    };
  });
}

async function grantAndRetry(payload) {
  const token = await getAccessTokenInteractive();
  const res = await fetch('/v1/permissions/grant', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({...payload, access_token: token}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = data.detail;
    const msg = (typeof detail === 'string' ? detail : null)
      || data.message || res.statusText;
    throw new Error(msg);
  }
  return data;
}
</script>
"""

def _status_pill(status: Optional[str]) -> str:
    s = (status or "—").lower()
    cls = "pill"
    if s in {"ok", "initial", "unpartitioned", "complete"}:
        cls += " ok"
    elif s in {"error", "not_found"}:
        cls += " error"
    elif s in {"out_of_range", "warning", "running"}:
        cls += " warn"
    return f'<span class="{cls}">{_esc(status or "—")}</span>'


def render_index(
    tables: List[Dict[str, Any]],
    *,
    user_email: Optional[str] = None,
    enabled_only: bool = False,
) -> str:
    rows_html = []
    for t in tables:
        href = (
            f"/ui/tables/{quote(t['project'], safe='')}/"
            f"{quote(t['dataset'], safe='')}/{quote(t['table'], safe='')}"
        )
        enabled = bool(t.get("enabled"))
        enabled_label = "yes" if enabled else "no"
        fqn = f"{t.get('project')}.{t.get('dataset')}.{t.get('table')}"
        if enabled:
            action = (
                f'<button type="button" class="btn danger btn-unregister"'
                f' data-project="{_esc(t.get("project"))}"'
                f' data-dataset="{_esc(t.get("dataset"))}"'
                f' data-table="{_esc(t.get("table"))}"'
                f' data-fqn="{_esc(fqn)}">Unregister</button>'
            )
        else:
            action = '<span class="muted">—</span>'
        rows_html.append(
            f"<tr>"
            f'<td><a class="rowlink" href="{href}">{_esc(t.get("table"))}</a>'
            f'<div class="muted mono">{_esc(t.get("project"))}.{_esc(t.get("dataset"))}</div></td>'
            f"<td>{_status_pill(t.get('last_status'))}</td>"
            f'<td class="mono">{_esc(t.get("last_pooled_at") or "—")}</td>'
            f"<td>{_esc(enabled_label)}</td>"
            f'<td class="muted">{_esc(t.get("partition_type") or "—")}'
            f' / {_esc(t.get("partition_granularity") or "—")}</td>'
            f"<td>{action}</td>"
            f"</tr>"
        )
    checked = "checked" if enabled_only else ""
    body = f"""
    <div class="toolbar">
      <form method="get" action="/" style="display:flex;gap:0.75rem;align-items:center;flex-wrap:wrap;">
        <input type="search" name="q" id="q" placeholder="Filter tables…" oninput="filterRows()"/>
        <label><input type="checkbox" name="enabled_only" value="1" {checked}
          onchange="this.form.submit()"/> enabled only</label>
        <span class="muted">{len(tables)} table(s)</span>
      </form>
    </div>
    <div id="listStatus"></div>
    <table id="tbl">
      <thead>
        <tr>
          <th>Table</th><th>Last status</th><th>Last pooled</th><th>Enabled</th><th>Partition</th><th></th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html) if rows_html else '<tr><td colspan="6" class="muted">No registered tables.</td></tr>'}
      </tbody>
    </table>
    <dialog class="confirm" id="unregisterDialog">
      <div class="stack">
        <div><b>Unregister table?</b></div>
        <div class="muted">Soft-disable scheduled pooling. History stays; you can register again later.</div>
        <div class="mono" id="unregisterFqn"></div>
        <div class="actions">
          <button class="btn" type="button" id="unregisterCancel">Cancel</button>
          <button class="btn danger" type="button" id="unregisterConfirm">Unregister</button>
        </div>
      </div>
    </dialog>
    <script>
      function filterRows() {{
        const q = (document.getElementById('q').value || '').toLowerCase();
        for (const tr of document.querySelectorAll('#tbl tbody tr')) {{
          tr.style.display = tr.innerText.toLowerCase().includes(q) ? '' : 'none';
        }}
      }}
      const dlg = document.getElementById('unregisterDialog');
      const fqnEl = document.getElementById('unregisterFqn');
      const statusEl = document.getElementById('listStatus');
      let pending = null;
      document.querySelectorAll('.btn-unregister').forEach(btn => {{
        btn.addEventListener('click', () => {{
          pending = {{
            project: btn.dataset.project,
            dataset: btn.dataset.dataset,
            table: btn.dataset.table,
            btn,
          }};
          fqnEl.textContent = btn.dataset.fqn || '';
          dlg.showModal();
        }});
      }});
      document.getElementById('unregisterCancel').onclick = () => {{ pending = null; dlg.close(); }};
      document.getElementById('unregisterConfirm').onclick = async () => {{
        if (!pending) {{ dlg.close(); return; }}
        const {{ project, dataset, table, btn }} = pending;
        pending = null;
        dlg.close();
        btn.disabled = true;
        statusEl.className = 'msg';
        statusEl.textContent = 'Unregistering ' + project + '.' + dataset + '.' + table + '…';
        try {{
          const res = await fetch('/v1/tables/unregister', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{ project, dataset, table }}),
          }});
          const data = await res.json().catch(() => ({{}}));
          if (!res.ok) throw new Error(data.detail || data.message || res.statusText);
          statusEl.className = 'msg ok';
          statusEl.textContent = 'Unregistered ' + project + '.' + dataset + '.' + table;
          const tr = btn.closest('tr');
          if (tr) {{
            const cells = tr.querySelectorAll('td');
            if (cells[3]) cells[3].textContent = 'no';
            const span = document.createElement('span');
            span.className = 'muted';
            span.textContent = '—';
            btn.replaceWith(span);
          }}
        }} catch (e) {{
          statusEl.className = 'msg err';
          statusEl.textContent = String(e.message || e);
          btn.disabled = false;
        }}
      }};
    </script>
    """
    return _layout("Registered tables", body, user_email=user_email, active="tables")


def render_table_detail(
    reg: Optional[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    *,
    project: str,
    dataset: str,
    table: str,
    watermark: Optional[str] = None,
    user_email: Optional[str] = None,
) -> str:
    if not reg:
        body = f'<p class="muted">Table not in registry: <span class="mono">{_esc(project)}.{_esc(dataset)}.{_esc(table)}</span></p>'
        return _layout("Not found", body, user_email=user_email, active="tables")

    kv = f"""
    <div class="panel">
      <div class="grid">
        <div class="kv"><div class="k">FQN</div><div class="v mono">{_esc(reg.get('full_table_name'))}</div></div>
        <div class="kv"><div class="k">Last status</div><div class="v">{_status_pill(reg.get('last_status'))}</div></div>
        <div class="kv"><div class="k">Last pooled</div><div class="v mono">{_esc(reg.get('last_pooled_at') or '—')}</div></div>
        <div class="kv"><div class="k">Watermark (CP)</div><div class="v mono">{_esc(watermark or '—')}</div></div>
        <div class="kv"><div class="k">Enabled</div><div class="v">{_esc('yes' if reg.get('enabled') else 'no')}</div></div>
        <div class="kv"><div class="k">Schedule group</div><div class="v">{_esc(reg.get('schedule_group'))}</div></div>
        <div class="kv"><div class="k">Partition</div><div class="v">{_esc(reg.get('partition_type'))} / {_esc(reg.get('partition_granularity'))} / {_esc(reg.get('partition_field'))}</div></div>
        <div class="kv"><div class="k">Change history</div><div class="v">{_esc(reg.get('change_history_enabled'))}</div></div>
      </div>
    </div>
    """

    log_rows = []
    for row in logs:
        ids = row.get("partition_ids")
        if ids is None:
            ids_s = "NULL (all)"
        elif len(ids) == 0:
            ids_s = "[]"
        else:
            ids_s = ", ".join(str(x) for x in ids[:40])
            if len(ids) > 40:
                ids_s += f" … (+{len(ids) - 40})"
        err = row.get("error") or ""
        err_html = (
            f'<pre class="err mono">{_esc(err)}</pre>' if err else ""
        )
        log_rows.append(
            f"<tr>"
            f'<td class="mono col-pooled">{_esc(row.get("pooled_at"))}</td>'
            f"<td>{_status_pill(row.get('status'))}</td>"
            f'<td class="mono col-delta">{_esc(row.get("delta_start_time"))}<br/>→ {_esc(row.get("delta_end_time"))}</td>'
            f"<td>{_esc(row.get('partitions_changed_cnt'))}</td>"
            f"<td>{_esc(row.get('rows_changed'))}</td>"
            f'<td class="col-ids"><pre class="ids mono">{_esc(ids_s)}</pre></td>'
            f'<td class="col-err muted">{err_html}</td>'
            f"</tr>"
        )

    body = f"""
    <p><a class="rowlink" href="/">← All tables</a></p>
    <h2 style="margin:0 0 0.75rem;font-size:1.15rem;">{_esc(table)}</h2>
    {kv}
    <h3 style="margin:1.25rem 0 0.5rem;font-size:1rem;">Recent pool windows</h3>
    <table class="table-logs">
      <colgroup>
        <col class="col-pooled"/><col style="width:7%"/><col class="col-delta"/>
        <col style="width:6%"/><col style="width:8%"/><col class="col-ids"/><col class="col-err"/>
      </colgroup>
      <thead>
        <tr>
          <th>Pooled at</th><th>Status</th><th>Delta window</th>
          <th># parts</th><th>Rows</th><th>Partition ids</th><th>Error</th>
        </tr>
      </thead>
      <tbody>
        {''.join(log_rows) if log_rows else '<tr><td colspan="7" class="muted">No log rows yet.</td></tr>'}
      </tbody>
    </table>
    """
    return _layout(f"{project}.{dataset}.{table}", body, user_email=user_email, active="tables")


def render_runs(runs: List[Dict[str, Any]], *, user_email: Optional[str] = None) -> str:
    rows = []
    for r in runs:
        href = f"/ui/runs/{r['id']}"
        rows.append(
            f"<tr>"
            f'<td><a class="rowlink" href="{href}">#{_esc(r.get("id"))}</a></td>'
            f'<td class="mono">{_esc(r.get("started_at") or "—")}</td>'
            f'<td class="mono">{_esc(r.get("completed_at") or "—")}</td>'
            f'<td class="mono">{_esc(r.get("watermark_from") or "—")}'
            f'<div class="muted">→ {_esc(r.get("watermark_to") or "—")}</div></td>'
            f"<td>{_status_pill(r.get('status'))}</td>"
            f"<td>{_esc(r.get('num_tables'))}</td>"
            f"<td>{_esc(r.get('num_warnings'))}</td>"
            f'<td class="muted">{_esc(r.get("trigger") or "")}</td>'
            f"</tr>"
        )
    body = f"""
    <div class="toolbar" style="justify-content:space-between;">
      <span class="muted">{len(runs)} run(s)</span>
      <button type="button" class="btn primary" id="btnRunNow">Run now</button>
    </div>
    <div id="runStatus"></div>
    <table>
      <thead>
        <tr>
          <th>Run</th><th>Started</th><th>Completed</th>
          <th>Watermarks</th><th>Status</th><th># tables</th><th># warnings</th><th>Trigger</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows) if rows else '<tr><td colspan="8" class="muted">No runs yet. Trigger a pool to populate.</td></tr>'}
      </tbody>
    </table>
    <dialog class="confirm" id="runDialog">
      <div class="stack">
        <div><b>Start a pool run?</b></div>
        <div class="muted">Pools all <b>enabled</b> registry tables (same as the scheduler target).
          This can take several minutes — keep this tab open until it finishes.</div>
        <label>Schedule group
          <div><input type="text" id="runScheduleGroup" value="default" style="min-width:12rem;"/></div>
        </label>
        <div class="actions">
          <button class="btn" type="button" id="runCancel">Cancel</button>
          <button class="btn primary" type="button" id="runConfirm">Start run</button>
        </div>
      </div>
    </dialog>
    <script>
      const dlg = document.getElementById('runDialog');
      const statusEl = document.getElementById('runStatus');
      const btn = document.getElementById('btnRunNow');
      document.getElementById('btnRunNow').onclick = () => dlg.showModal();
      document.getElementById('runCancel').onclick = () => dlg.close();
      document.getElementById('runConfirm').onclick = async () => {{
        const group = (document.getElementById('runScheduleGroup').value || 'default').trim();
        dlg.close();
        btn.disabled = true;
        statusEl.className = 'msg';
        statusEl.textContent = 'Pooling enabled tables (group=' + group + ')…';
        try {{
          const res = await fetch('/v1/pool', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{ schedule_group: group }}),
          }});
          const data = await res.json().catch(() => ({{}}));
          if (!res.ok) throw new Error(data.detail || data.message || res.statusText);
          const runMeta = data.run || {{}};
          const runId = runMeta.run_id || runMeta.id;
          statusEl.className = 'msg ok';
          statusEl.textContent = 'Run finished' + (runId ? (' #' + runId) : '')
            + ' — ' + (data.count || 0) + ' table(s). Redirecting…';
          if (runId) {{
            setTimeout(() => {{ window.location.href = '/ui/runs/' + runId; }}, 600);
          }} else {{
            setTimeout(() => {{ window.location.reload(); }}, 800);
          }}
        }} catch (e) {{
          statusEl.className = 'msg err';
          statusEl.textContent = String(e.message || e);
          btn.disabled = false;
        }}
      }};
    </script>
    """
    return _layout("Pool runs", body, user_email=user_email, active="runs")


def render_run_detail(
    run: Optional[Dict[str, Any]],
    events: List[Dict[str, Any]],
    *,
    user_email: Optional[str] = None,
) -> str:
    if not run:
        body = '<p class="muted">Run not found.</p>'
        return _layout("Run not found", body, user_email=user_email, active="runs")

    kv = f"""
    <div class="panel">
      <div class="grid">
        <div class="kv"><div class="k">Run id</div><div class="v">#{_esc(run.get('id'))}</div></div>
        <div class="kv"><div class="k">Status</div><div class="v">{_status_pill(run.get('status'))}</div></div>
        <div class="kv"><div class="k">Started</div><div class="v mono">{_esc(run.get('started_at'))}</div></div>
        <div class="kv"><div class="k">Completed</div><div class="v mono">{_esc(run.get('completed_at') or '—')}</div></div>
        <div class="kv"><div class="k">Watermark from</div><div class="v mono">{_esc(run.get('watermark_from') or '—')}</div></div>
        <div class="kv"><div class="k">Watermark to</div><div class="v mono">{_esc(run.get('watermark_to') or '—')}</div></div>
        <div class="kv"><div class="k">Tables</div><div class="v">{_esc(run.get('num_tables'))}</div></div>
        <div class="kv"><div class="k">Warnings / errors</div><div class="v">{_esc(run.get('num_warnings'))} / {_esc(run.get('num_errors'))}</div></div>
        <div class="kv"><div class="k">Trigger</div><div class="v">{_esc(run.get('trigger'))}</div></div>
        <div class="kv"><div class="k">Summary</div><div class="v mono">{_esc(run.get('summary') or '—')}</div></div>
      </div>
    </div>
    """
    lines = []
    for e in events:
        lvl = (e.get("level") or "INFO").upper()
        extra = ""
        if e.get("full_table_name"):
            extra = f' <span class="muted">[{_esc(e.get("full_table_name"))}]</span>'
        lines.append(
            f'<div class="line">'
            f'<div class="mono muted">{_esc(e.get("logged_at"))}</div>'
            f'<div class="lvl-{_esc(lvl)}">[{_esc(lvl)}]</div>'
            f'<div>{_esc(e.get("message"))}{extra}</div>'
            f"</div>"
        )
    body = f"""
    <p><a class="rowlink" href="/ui/runs">← All runs</a></p>
    <h2 style="margin:0 0 0.75rem;font-size:1.15rem;">Run #{_esc(run.get('id'))}</h2>
    {kv}
    <h3 style="margin:1.25rem 0 0.5rem;font-size:1rem;">Run log</h3>
    <div class="log">
      {''.join(lines) if lines else '<div class="line"><div></div><div></div><div class="muted">No events.</div></div>'}
    </div>
    """
    return _layout(f"Run #{run.get('id')}", body, user_email=user_email, active="runs")


def render_register(
    *,
    user_email: Optional[str] = None,
    oauth_client_id: str = "",
    sa_email: str = "",
) -> str:
    body = f"""
    <div class="stack" style="max-width:720px;">
      <p class="muted">Register a dataset (<code>project.dataset</code>) or a single table
      (<code>project.dataset.table</code>). Access is granted at <b>dataset or project</b>
      level only (never table). Pooler SA: <span class="mono">{_esc(sa_email)}</span></p>
      <label>Target
        <div><input type="text" id="target" placeholder="my-gcp-project.analytics or my-gcp-project.analytics.orders"/></div>
      </label>
      <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
        <button class="btn" type="button" id="btnPreview">List tables</button>
        <button class="btn primary" type="button" id="btnRegister">Register selected</button>
      </div>
      <div id="status"></div>
      <div id="tablePick" class="table-pick" style="display:none;"></div>
      <div id="needs"></div>
    </div>
    {_GRANT_JS}
    <script>
    const statusEl = document.getElementById('status');
    const pickEl = document.getElementById('tablePick');
    const needsEl = document.getElementById('needs');
    function setStatus(html, cls) {{
      statusEl.className = 'msg ' + (cls || '');
      statusEl.innerHTML = html;
    }}
    function renderNeeds(needs) {{
      if (!needs || !needs.length) {{ needsEl.innerHTML = ''; return; }}
      needsEl.innerHTML = needs.map((n, i) => `
        <div class="panel">
          <div><b>Missing:</b> <span class="mono">${{n.required_role}}</span>
            for <span class="mono">${{n.sa_email}}</span> on
            <span class="mono">${{n.resource}}</span></div>
          <div class="muted">${{n.reason || ''}}</div>
          <pre class="cmd">${{n.gcloud}}</pre>
          <button class="btn primary" type="button" data-i="${{i}}">Grant &amp; retry</button>
        </div>`).join('');
      needsEl.querySelectorAll('button[data-i]').forEach(btn => {{
        btn.onclick = async () => {{
          const n = needs[Number(btn.dataset.i)];
          btn.disabled = true;
          try {{
            const out = await grantAndRetry({{
              project: n.project, dataset: n.dataset, sa_email: n.sa_email,
              required_role: n.required_role, scope: n.scope,
              retry_target: document.getElementById('target').value,
            }});
            setStatus('Granted. ' + (out.message || 'Retry succeeded.'), 'ok');
            if (out.tables) showTables(out.tables);
            renderNeeds(out.needs || []);
          }} catch (e) {{
            setStatus(String(e.message || e), 'err');
          }} finally {{ btn.disabled = false; }}
        }};
      }});
    }}
    function showTables(tables) {{
      pickEl.style.display = 'block';
      pickEl.innerHTML = tables.map(t =>
        `<label style="display:block;"><input type="checkbox" name="t" value="${{t}}" checked/> ${{t}}</label>`
      ).join('') || '<span class="muted">No base tables found.</span>';
    }}
    document.getElementById('btnPreview').onclick = async () => {{
      setStatus('Loading…');
      needsEl.innerHTML = '';
      const res = await fetch('/v1/register/preview', {{
        method: 'POST', headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{ target: document.getElementById('target').value }}),
      }});
      const data = await res.json().catch(() => ({{}}));
      if (!res.ok) {{
        setStatus(data.detail || data.message || 'Preview failed', 'err');
        renderNeeds(data.needs || []);
        return;
      }}
      setStatus(`Found ${{data.tables.length}} table(s) in ${{data.project}}.${{data.dataset}}`, 'ok');
      showTables(data.tables);
      if (data.table) {{
        // single-table target
        pickEl.innerHTML = `<label><input type="checkbox" name="t" value="${{data.table}}" checked/> ${{data.table}}</label>`;
        pickEl.style.display = 'block';
      }}
    }};
    document.getElementById('btnRegister').onclick = async () => {{
      const tables = [...pickEl.querySelectorAll('input[name=t]:checked')].map(x => x.value);
      setStatus('Registering…');
      const res = await fetch('/v1/register', {{
        method: 'POST', headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{
          target: document.getElementById('target').value,
          tables: tables.length ? tables : null,
        }}),
      }});
      const data = await res.json().catch(() => ({{}}));
      if (!res.ok) {{
        setStatus(data.detail || data.message || 'Register failed', 'err');
        renderNeeds(data.needs || []);
        return;
      }}
      setStatus(`Registered ${{data.registered}} table(s). Failed: ${{data.failed}}.`, data.failed ? 'err' : 'ok');
      renderNeeds(data.needs || []);
    }};
    </script>
    """
    return _layout(
        "Register tables",
        body,
        user_email=user_email,
        active="register",
        oauth_client_id=oauth_client_id,
    )


def render_permissions(
    issues: List[Dict[str, Any]],
    *,
    user_email: Optional[str] = None,
    oauth_client_id: str = "",
) -> str:
    rows = []
    for issue in issues:
        resource = issue.get("resource") or f"{issue.get('project')}.{issue.get('dataset')}"
        rows.append(
            f"<tr data-id='{issue['id']}'>"
            f"<td class='mono'>{_esc(resource)}</td>"
            f"<td class='mono'>{_esc(issue.get('required_role'))}</td>"
            f"<td class='mono'>{_esc(issue.get('sa_email'))}</td>"
            f"<td class='muted'>{_esc((issue.get('last_error') or '')[:160])}</td>"
            f"<td class='mono'>{_esc(issue.get('last_seen_at'))}</td>"
            f"<td><button class='btn' type='button' data-open='{issue['id']}'>Details</button></td>"
            f"</tr>"
        )
    issues_json = json.dumps(issues)
    body = f"""
    <p class="muted">Open permission gaps seen by the pooler SA (dataset/project level).
    Click Details for the gcloud command and Grant &amp; retry.</p>
    <div class="toolbar"><span class="muted">{len(issues)} open issue(s)</span></div>
    <table>
      <thead>
        <tr><th>Resource</th><th>Role</th><th>SA</th><th>Last error</th><th>Last seen</th><th></th></tr>
      </thead>
      <tbody>
        {''.join(rows) if rows else '<tr><td colspan="6" class="muted">No open permission issues.</td></tr>'}
      </tbody>
    </table>
    <div id="detail" style="margin-top:1rem;"></div>
    {_GRANT_JS}
    <script>
    const issues = {issues_json};
    const byId = Object.fromEntries(issues.map(i => [String(i.id), i]));
    const detail = document.getElementById('detail');
    document.querySelectorAll('button[data-open]').forEach(btn => {{
      btn.onclick = () => {{
        const n = byId[btn.dataset.open];
        if (!n) return;
        detail.innerHTML = `
          <div class="panel stack">
            <div><b>${{n.required_role}}</b> for <span class="mono">${{n.sa_email}}</span>
              on <span class="mono">${{n.resource}}</span></div>
            <div class="muted">${{(n.last_error || '').replace(/</g,'&lt;')}}</div>
            <pre class="cmd">${{(n.gcloud || '').replace(/</g,'&lt;')}}</pre>
            <button class="btn primary" type="button" id="grantBtn">Grant &amp; retry probe</button>
            <div id="grantStatus"></div>
          </div>`;
        document.getElementById('grantBtn').onclick = async () => {{
          const gs = document.getElementById('grantStatus');
          gs.className = 'msg'; gs.textContent = 'Working…';
          try {{
            const out = await grantAndRetry({{
              project: n.project, dataset: n.dataset, sa_email: n.sa_email,
              required_role: n.required_role, scope: n.scope, issue_id: n.id,
            }});
            gs.className = 'msg ok';
            gs.textContent = out.message || 'Granted and issue resolved.';
            if (out.resolved) btn.closest('tr')?.remove();
          }} catch (e) {{
            gs.className = 'msg err';
            gs.textContent = String(e.message || e);
          }}
        }};
      }};
    }});
    </script>
    """
    return _layout(
        "Permission issues",
        body,
        user_email=user_email,
        active="permissions",
        oauth_client_id=oauth_client_id,
    )
