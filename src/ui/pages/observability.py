"""Observability page — Run history table, Plotly metrics dashboard, Chatbot."""
from __future__ import annotations
import json
import logging
import os

import streamlit as st
from src.ui.utils.api_client import load_run_logs

logger = logging.getLogger(__name__)

GRAFANA_URL        = os.getenv("GRAFANA_BASE_URL", "")
GRAFANA_PUBLIC_URL = os.getenv("GRAFANA_PUBLIC_URL", "https://etlobservability.grafana.net/dashboard/snapshot/bkH00mlIW8VDwLulhVmpaeCw4qRHjk4F")


# ── Postgres data loaders ─────────────────────────────────────────────────────

def _load_runs_from_postgres() -> list[dict]:
    try:
        import psycopg2
        import psycopg2.extras
        pg_dsn = os.getenv("UC2_PG_DSN")
        if not pg_dsn:
            return []
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT run_id, source, status, ts, payload
            FROM audit_events
            WHERE event_type = 'run_completed'
            ORDER BY ts DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        result = []
        for row in rows:
            payload = row.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            record = {
                "run_id":      row["run_id"],
                "source_name": payload.get("source_name") or row["source"],
                "status":      row["status"] or payload.get("status", ""),
                "timestamp":   row["ts"].isoformat() if row["ts"] else "",
                **{k: v for k, v in payload.items() if k not in ("run_id", "source_name", "status")},
            }
            result.append(record)
        return result
    except Exception as exc:
        logger.warning(f"Postgres run load failed: {exc}")
        return []


def _load_blocks_from_postgres():
    try:
        import psycopg2
        import pandas as pd
        pg_dsn = os.getenv("UC2_PG_DSN")
        if not pg_dsn:
            return None
        conn = psycopg2.connect(pg_dsn)
        df = pd.read_sql("""
            SELECT run_id, source, block_name, block_seq, duration_ms, dq_score, rows_in, rows_out
            FROM block_trace
            ORDER BY ts DESC
        """, conn)
        conn.close()
        return df
    except Exception as exc:
        logger.warning(f"Postgres block load failed: {exc}")
        return None


# ── Grafana-equivalent Plotly dashboard ───────────────────────────────────────

def _render_grafana_dashboard_plotly() -> None:
    import pandas as pd
    import plotly.graph_objects as go

    # Colors matching Grafana dashboard JSON exactly
    C_RED    = "#F2495C"
    C_YELLOW = "#FAB90B"
    C_GREEN  = "#37872D"
    C_BLUE   = "#1F60C4"
    C_PURPLE = "#8F3BB8"

    LAYOUT = dict(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
                  size=13, color="#212529"),
        margin=dict(l=10, r=10, t=36, b=10),
    )

    def _dq_color(v):
        if v is None or v != v: return C_RED
        return C_GREEN if v >= 80 else (C_YELLOW if v >= 60 else C_RED)

    def _threshold_color(v, warn, crit):
        if v is None or v != v: return C_GREEN
        return C_RED if v >= crit else (C_YELLOW if v >= warn else C_GREEN)

    def _horiz_bars(srcs, vals, colors, unit="", height=None):
        labels = [f"{v:,.1f}{unit}" if isinstance(v, float) else (f"{int(v):,}{unit}" if v is not None else "—") for v in vals]
        fig = go.Figure(go.Bar(
            x=vals, y=srcs, orientation='h',
            marker_color=colors,
            text=labels, textposition="outside",
            cliponaxis=False,
        ))
        fig.update_layout(
            **LAYOUT,
            height=height or max(110, 58 * len(srcs)),
            showlegend=False,
            xaxis=dict(gridcolor="#dee2e6", zeroline=False),
            yaxis=dict(automargin=True),
        )
        return fig

    # ── load data ─────────────────────────────────────────────────────────────
    logs = load_run_logs() or _load_runs_from_postgres()
    if not logs:
        st.info("No pipeline runs recorded yet. Run a pipeline to see metrics here.")
        return

    df = pd.DataFrame(logs)
    df["source_name"] = df.get("source_name", df.get("source", "unknown"))
    df["timestamp"] = pd.to_datetime(df.get("timestamp", pd.Series(dtype="str")), errors="coerce", utc=True)
    df = df.sort_values("timestamp")
    latest = df.groupby("source_name").last().reset_index()
    sources = latest["source_name"].tolist()

    def _col(name, default=None):
        return [latest[name].iloc[i] if name in latest.columns else default
                for i in range(len(latest))]

    # ── header banner ─────────────────────────────────────────────────────────
    st.markdown("""
    <div class="card" style="margin-bottom:16px;padding:14px 18px;">
      <div style="font-size:15px;line-height:1.75;color:var(--text-muted);">
        Raw data from <strong>USDA</strong>, <strong>openFDA</strong>,
        <strong>OpenFoodFacts</strong>, and <strong>ESCI</strong> cleaned block-by-block,
        then enriched via a three-tier cascade
        (<strong>S1 rules → S2 KNN → S3 LLM</strong>).
        The <strong>DQ score</strong> (0–100) measures completeness &amp; validity —
        watch it rise from <em>Before</em> to <em>After</em>.
      </div>
    </div>""", unsafe_allow_html=True)

    # ── Panel 1: Final DQ Score per Source ────────────────────────────────────
    st.markdown('<div class="card-title" style="margin-bottom:6px;">Final DQ Score per Source (Post-Pipeline)</div>', unsafe_allow_html=True)
    st.caption("Green ≥ 80 = production-ready · Yellow 60–80 = usable · Red < 60 = needs attention")
    dq_post = _col("dq_score_post")
    fig1 = _horiz_bars(sources, dq_post, [_dq_color(v) for v in dq_post], height=max(120, 62 * len(sources)))
    fig1.update_layout(xaxis=dict(range=[0, 115], gridcolor="#dee2e6", zeroline=False))
    fig1.add_vline(x=60, line_color=C_YELLOW, line_width=1, line_dash="dot",
                   annotation_text="60", annotation_position="top right")
    fig1.add_vline(x=80, line_color=C_GREEN, line_width=1, line_dash="dot",
                   annotation_text="80", annotation_position="top right")
    st.plotly_chart(fig1, use_container_width=True)

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

    # ── Row 2: DQ Before/After | Enrichment Tiers ─────────────────────────────
    col_dq, col_enrich = st.columns(2)

    with col_dq:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">DQ Score: Before vs After Pipeline</div>', unsafe_allow_html=True)
        dq_pre = _col("dq_score_pre")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name="Before", x=sources, y=dq_pre, marker_color=C_RED,
            text=[f"{v:.1f}" if v else "—" for v in dq_pre], textposition="outside",
        ))
        fig2.add_trace(go.Bar(
            name="After", x=sources, y=dq_post, marker_color=C_GREEN,
            text=[f"{v:.1f}" if v else "—" for v in dq_post], textposition="outside",
        ))
        fig2.update_layout(
            **LAYOUT, barmode="group", height=310, showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.28, x=0.3),
            yaxis=dict(range=[0, 115], gridcolor="#dee2e6", title="Score"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_enrich:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">Enrichment Tier Breakdown (totals across all runs)</div>', unsafe_allow_html=True)
        s1  = sum((r.get("enrichment_stats") or {}).get("deterministic", 0) or 0 for r in logs)
        s2  = sum((r.get("enrichment_stats") or {}).get("embedding",     0) or 0 for r in logs)
        s3  = sum((r.get("enrichment_stats") or {}).get("llm",           0) or 0 for r in logs)
        unr = sum((r.get("enrichment_stats") or {}).get("unresolved",    0) or 0 for r in logs)
        fig3 = go.Figure(go.Bar(
            x=["S1 rules", "S2 KNN", "S3 LLM", "Unresolved"],
            y=[s1, s2, s3, unr],
            marker_color=[C_GREEN, C_BLUE, C_PURPLE, C_RED],
            text=[f"{v:,}" for v in [s1, s2, s3, unr]],
            textposition="outside",
        ))
        fig3.update_layout(
            **LAYOUT, height=310, showlegend=False,
            yaxis=dict(gridcolor="#dee2e6", title="Rows resolved"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

    # ── Row 3: 4 horizontal bargauges ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    h = max(110, 58 * len(sources))

    with c1:
        st.markdown('<div class="card-title" style="font-size:12px;margin-bottom:4px;">Rows In (raw)</div>', unsafe_allow_html=True)
        vals = _col("rows_in", 0)
        st.plotly_chart(_horiz_bars(sources, vals, [C_BLUE]*len(sources), height=h), use_container_width=True)

    with c2:
        st.markdown('<div class="card-title" style="font-size:12px;margin-bottom:4px;">Rows Out (clean)</div>', unsafe_allow_html=True)
        vals = _col("rows_out", 0)
        st.plotly_chart(_horiz_bars(sources, vals, [C_GREEN]*len(sources), height=h), use_container_width=True)

    with c3:
        st.markdown('<div class="card-title" style="font-size:12px;margin-bottom:4px;">Rows Quarantined</div>', unsafe_allow_html=True)
        vals = _col("rows_quarantined", 0)
        st.plotly_chart(_horiz_bars(sources, vals,
            [_threshold_color(v, 100, 1000) for v in vals], height=h), use_container_width=True)

    with c4:
        st.markdown('<div class="card-title" style="font-size:12px;margin-bottom:4px;">Run Duration (s)</div>', unsafe_allow_html=True)
        vals = _col("duration_seconds", 0)
        st.plotly_chart(_horiz_bars(sources, vals, [C_PURPLE]*len(sources), unit="s", height=h), use_container_width=True)

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

    # ── Row 4: LLM Cost | LLM Calls ───────────────────────────────────────────
    col_cost, col_calls = st.columns(2)

    with col_cost:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">LLM Cost per Source (USD)</div>', unsafe_allow_html=True)
        st.caption("Green < $0.10 · Yellow $0.10–$1 · Red ≥ $1 — cost cascade keeps this near zero")
        vals = _col("cost_usd", 0)
        st.plotly_chart(_horiz_bars(sources, vals,
            [_threshold_color(v, 0.1, 1.0) for v in vals]), use_container_width=True)

    with col_calls:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">LLM Calls per Source (count)</div>', unsafe_allow_html=True)
        st.caption("Zero calls = S1/S2 resolved every row (healthy). Spikes = S1/S2 failing.")
        vals = _col("llm_calls", 0)
        st.plotly_chart(_horiz_bars(sources, vals,
            [_threshold_color(v, 100, 1000) for v in vals]), use_container_width=True)

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

    # ── Row 5: Corpus Size | Block Duration ───────────────────────────────────
    col_corpus, col_block = st.columns(2)

    with col_corpus:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">Corpus Size per Source (FAISS vectors)</div>', unsafe_allow_html=True)
        st.caption("Grows each run as resolved rows feed back — larger corpus = cheaper future runs")
        corpus_vals = [
            (r.get("enrichment_stats") or {}).get("corpus_size_after") or 0
            for r in [
                next((r for r in reversed(logs)
                      if (r.get("source_name") or r.get("source", "")) == s), {})
                for s in sources
            ]
        ]
        fig_corpus = go.Figure(go.Bar(
            x=corpus_vals, y=sources, orientation='h',
            marker=dict(color=corpus_vals, colorscale=[[0, "#5794F2"], [1, "#8F3BB8"]], showscale=False),
            text=[f"{v:,}" for v in corpus_vals], textposition="outside", cliponaxis=False,
        ))
        fig_corpus.update_layout(
            **LAYOUT, height=max(110, 58 * len(sources)), showlegend=False,
            xaxis=dict(title="Vectors", gridcolor="#dee2e6", zeroline=False),
        )
        st.plotly_chart(fig_corpus, use_container_width=True)

    with col_block:
        st.markdown('<div class="card-title" style="margin-bottom:6px;">Block Duration (ms) — latest run</div>', unsafe_allow_html=True)
        st.caption("Long bars = optimisation candidates. Pairs with DQ delta: cost vs. benefit per block.")
        block_df = _load_blocks_from_postgres()
        if block_df is not None and not block_df.empty and "duration_ms" in block_df.columns:
            run_ids = df["run_id"].dropna().tolist() if "run_id" in df.columns else []
            latest_run_id = run_ids[-1] if run_ids else None
            bdf = (block_df[block_df["run_id"] == latest_run_id]
                   if latest_run_id and latest_run_id in block_df["run_id"].values
                   else block_df.sort_values("block_seq").tail(20))
            bdf = bdf.sort_values("block_seq")
            fig_block = go.Figure(go.Bar(
                x=bdf["block_name"].tolist(), y=bdf["duration_ms"].tolist(),
                marker=dict(color=bdf["duration_ms"].tolist(),
                            colorscale="YlOrRd", showscale=False),
                text=[f"{v:.0f}ms" for v in bdf["duration_ms"].tolist()],
                textposition="outside", cliponaxis=False,
            ))
            fig_block.update_layout(
                **LAYOUT, height=310, showlegend=False,
                xaxis=dict(tickangle=40, title=""),
                yaxis=dict(title="Duration (ms)", gridcolor="#dee2e6"),
            )
            st.plotly_chart(fig_block, use_container_width=True)
        else:
            st.info("No block trace data yet — run a pipeline with UC2 Kafka consumer active.")

    # ── external Grafana link (optional) ──────────────────────────────────────
    if GRAFANA_URL:
        st.markdown(
            f'<div style="margin-top:10px;font-size:13px;color:var(--text-dim);">'
            f'Extended dashboard: <a href="{GRAFANA_URL}" target="_blank" '
            f'style="color:var(--accent);font-weight:600;">Open Grafana ↗</a></div>',
            unsafe_allow_html=True,
        )


def _status_badge(s: str) -> str:
    cls = {"success": "success", "error": "error", "running": "running"}.get(s, "warning")
    return f'<span class="badge {cls}">{s}</span>'


def _domain_badge(d: str) -> str:
    colors = {"nutrition": "info", "safety": "error", "pricing": "warning", "retail": "purple"}
    cls = colors.get((d or "").lower(), "info")
    return f'<span class="badge {cls}">{d or "—"}</span>'


def _dq_arrow(pre, post, delta) -> str:
    pre_s  = f"{pre:.1f}"  if pre  is not None else "—"
    post_s = f"{post:.1f}" if post is not None else "—"
    post_cls = "after" if post is not None else "after na"
    d_html = ""
    if delta is not None:
        sign = "+" if delta >= 0 else ""
        color = "var(--green)" if delta >= 0 else "var(--red)"
        d_html = f'<span class="delta" style="color:{color}">({sign}{delta:.1f})</span>'
    return f'<span class="dq-arrow"><span class="before">{pre_s}</span><span class="arrow"> → </span><span class="{post_cls}">{post_s}</span> {d_html}</span>'


def render_observability():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Observability</div>
        <div class="page-subtitle">Pipeline run history, Grafana dashboards, and AI-powered chatbot</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["Run History", "Grafana", "Chatbot"])

    # ── Tab 0: Run History ────────────────────────────────────────────────────
    with tabs[0]:
        logs = load_run_logs()

        # Filters
        fc1, fc2, fc3, _ = st.columns([2, 2, 2, 3])
        with fc1:
            sources = sorted({r.get("source_name", r.get("source", "")) for r in logs if r.get("source_name") or r.get("source")})
            sel_src = st.selectbox("Source", ["All"] + sources, key="obs_src")
        with fc2:
            domains = sorted({r.get("domain", "") for r in logs if r.get("domain")})
            sel_dom = st.selectbox("Domain", ["All"] + domains, key="obs_dom")
        with fc3:
            sel_status = st.selectbox("Status", ["All", "success", "error", "running"], key="obs_status")

        filtered = logs
        if sel_src != "All":
            filtered = [r for r in filtered if r.get("source_name", r.get("source", "")) == sel_src]
        if sel_dom != "All":
            filtered = [r for r in filtered if r.get("domain", "") == sel_dom]
        if sel_status != "All":
            filtered = [r for r in filtered if r.get("status", "") == sel_status]

        total_ok  = sum(1 for r in filtered if r.get("status") == "success")
        total_err = sum(1 for r in filtered if r.get("status") == "error")

        st.markdown(f"""
        <div style="display:flex;gap:10px;align-items:center;margin-bottom:14px;">
          <span class="badge info">{len(filtered)} runs</span>
          <span class="badge success">{total_ok} success</span>
          <span class="badge error">{total_err} error</span>
        </div>""", unsafe_allow_html=True)

        rows_html = ""
        for r in filtered:
            src     = r.get("source_name", r.get("source", "—"))
            domain  = r.get("domain", "")
            status  = r.get("status", "")
            pre     = r.get("dq_score_pre")
            post    = r.get("dq_score_post")
            delta   = r.get("dq_delta")
            ts      = r.get("timestamp", "")[:19].replace("T", " ")
            dur     = r.get("duration_seconds")
            rows_in = r.get("rows_in", 0) or 0
            quaran  = r.get("rows_quarantined", 0) or 0
            run_id  = r.get("run_id", "")[:8]
            dur_s   = f"{dur:.1f}s" if dur is not None else "—"

            enrich  = r.get("enrichment_stats", {})
            s1 = enrich.get("deterministic", enrich.get("s1", 0)) or 0
            s2 = enrich.get("embedding",     enrich.get("s2", 0)) or 0
            s3 = enrich.get("llm",           enrich.get("s3", 0)) or 0

            rows_html += f"""
            <tr>
              <td><span class="mono tc-dim">{run_id}</span></td>
              <td><span class="mono">{src}</span></td>
              <td>{_domain_badge(domain)}</td>
              <td>{_status_badge(status)}</td>
              <td>{_dq_arrow(pre, post, delta)}</td>
              <td class="mono">{rows_in:,}</td>
              <td class="{'tc-red' if quaran > 0 else 'tc-dim'}">{quaran:,}</td>
              <td class="tc-green">{s1:,}</td>
              <td class="tc-accent">{s2:,}</td>
              <td class="tc-amber">{s3:,}</td>
              <td class="tc-dim">{dur_s}</td>
              <td class="tc-dim" style="font-size:12px;">{ts}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card" style="overflow-x:auto;">
          <div class="card-title">Pipeline Runs — {len(filtered)} records</div>
          <table class="data-table">
            <thead><tr>
              <th>Run ID</th><th>Source</th><th>Domain</th><th>Status</th>
              <th>DQ Score</th><th>Rows In</th><th>Quarantined</th>
              <th>S1</th><th>S2</th><th>S3</th><th>Duration</th><th>Timestamp</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

    # ── Tab 1: Grafana dashboard screenshot + link ────────────────────────────
    with tabs[1]:
        import base64
        from pathlib import Path
        _img_path = Path(__file__).parent.parent / "assets" / "grafana_dashboard.png"
        _img_b64 = ""
        if _img_path.exists():
            _img_b64 = base64.b64encode(_img_path.read_bytes()).decode()

        st.markdown(f"""
        <div class="card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:12px;">
            <div class="card-title" style="margin:0;">Grafana — ETL Pipeline Observability</div>
            <a href="{GRAFANA_PUBLIC_URL}" target="_blank" style="text-decoration:none;">
              <div style="background:var(--accent);color:#fff;font-weight:700;font-size:13px;
                          padding:8px 18px;border-radius:6px;white-space:nowrap;">
                Open Live Dashboard ↗
              </div>
            </a>
          </div>
          {"" if not _img_b64 else f'''
          <a href="{GRAFANA_PUBLIC_URL}" target="_blank" style="text-decoration:none;display:block;">
            <img src="data:image/png;base64,{_img_b64}"
                 style="width:100%;border-radius:8px;border:1px solid var(--border);
                        display:block;cursor:pointer;"
                 alt="Grafana ETL Pipeline Observability Dashboard"/>
          </a>'''}
          <div style="margin-top:16px;display:flex;gap:24px;flex-wrap:wrap;">
            <div style="text-align:center;flex:1;">
              <div style="font-size:20px;font-weight:800;color:var(--green);">3</div>
              <div style="font-size:12px;color:var(--text-muted);">Sources</div>
            </div>
            <div style="text-align:center;flex:1;">
              <div style="font-size:20px;font-weight:800;color:var(--accent);">97,813</div>
              <div style="font-size:12px;color:var(--text-muted);">Enriched Rows</div>
            </div>
            <div style="text-align:center;flex:1;">
              <div style="font-size:20px;font-weight:800;color:var(--amber);">70.9</div>
              <div style="font-size:12px;color:var(--text-muted);">Avg DQ Post</div>
            </div>
            <div style="text-align:center;flex:1;">
              <div style="font-size:20px;font-weight:800;color:var(--text);">162,441</div>
              <div style="font-size:12px;color:var(--text-muted);">Corpus Vectors</div>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)

    # ── Tab 2: Chatbot ────────────────────────────────────────────────────────
    with tabs[2]:
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []

        # MCP server status pill
        try:
            import requests
            r = requests.get("http://localhost:8001/health", timeout=2)
            mcp_ok = r.status_code < 400
        except Exception:
            mcp_ok = False

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
          <div class="health-pill">
            <span class="health-dot {'ok' if mcp_ok else 'warn'}"></span>MCP Server
          </div>
          <span style="font-size:13px;color:var(--text-dim);">
            Ask questions about pipeline runs, DQ scores, anomalies, and cost.
          </span>
        </div>""", unsafe_allow_html=True)

        # Render existing chat history
        for msg in st.session_state.chat_history:
            role = msg["role"]
            content = msg["content"]
            cited = msg.get("cited_runs", [])
            if role == "user":
                st.markdown(f"""
                <div style="display:flex;justify-content:flex-end;margin-bottom:10px;">
                  <div style="background:var(--accent-dim);border:1px solid rgba(25,113,194,.15);
                              border-radius:var(--radius);padding:10px 14px;
                              max-width:75%;font-size:14px;color:var(--text);">{content}</div>
                </div>""", unsafe_allow_html=True)
            else:
                cited_html = ""
                if cited:
                    run_chips = " ".join(f'<span class="run-chip">{r}</span>' for r in cited[:6])
                    cited_html = f'<div style="margin-top:8px;font-size:12px;color:var(--text-dim);">Cited runs: {run_chips}</div>'
                st.markdown(f"""
                <div style="display:flex;justify-content:flex-start;margin-bottom:10px;">
                  <div class="chat-bubble" style="max-width:80%;">{content}{cited_html}</div>
                </div>""", unsafe_allow_html=True)

        # Suggested prompts — always visible
        suggestions = [
            ("📊", "What's the overall pipeline success rate?"),
            ("🔴", "Which source has the most quarantined rows?"),
            ("📈", "Which source improved DQ the most?"),
            ("💰", "What was the total LLM cost across all runs?"),
            ("⚠️",  "Show me all failed runs and their errors"),
            ("🧬", "How many rows were enriched via S3 LLM?"),
        ]
        r1, r2, r3 = st.columns(3)
        r4, r5, r6 = st.columns(3)
        for col, (icon, prompt) in zip([r1, r2, r3, r4, r5, r6], suggestions):
            with col:
                if st.button(f"{icon} {prompt}", key=f"sugg_{prompt[:24]}", use_container_width=True):
                    st.session_state._pending_chat = prompt
                    st.rerun()

        # Input
        user_input = st.chat_input("Ask about pipeline runs, quality, cost…")
        pending = st.session_state.pop("_pending_chat", None)
        query = user_input or pending

        if query:
            st.session_state.chat_history.append({"role": "user", "content": query})
            with st.spinner("Analyzing run history…"):
                answer, cited = _chatbot_query(query)
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": answer,
                "cited_runs": cited,
            })
            st.rerun()

        if st.session_state.chat_history:
            if st.button("Clear chat", key="clear_chat"):
                st.session_state.chat_history = []
                st.rerun()


_PIPELINE_KEYWORDS = {
    "run", "pipeline", "dq", "quality", "score", "enrich", "llm", "s1", "s2", "s3",
    "tier", "quarantin", "flagged", "reject", "success", "fail", "error", "crash",
    "source", "rows", "cost", "usd", "spend", "anomaly", "block", "dedup", "schema",
    "data", "record", "batch", "dag", "airflow", "kafka", "bronze", "silver", "gold",
    "usda", "fda", "off", "esci", "nutrition", "safety", "status", "broken",
}


def _is_pipeline_question(q: str) -> bool:
    words = set(q.lower().split())
    return bool(words & _PIPELINE_KEYWORDS)


def _chatbot_query(query: str) -> tuple[str, list[str]]:
    """Try ObservabilityChatbot, fallback to rich log analytics."""
    if not _is_pipeline_question(query):
        return (
            "I only answer questions about pipeline runs, DQ scores, enrichment, "
            "anomalies, and cost.\n\n"
            "Try asking: *success rate*, *DQ improvement*, *quarantine breakdown*, "
            "*enrichment tiers*, *LLM cost*, *failed runs*, or *source breakdown*."
        ), []

    try:
        from src.uc2_observability.rag_chatbot import ObservabilityChatbot
        bot = ObservabilityChatbot()
        resp = bot.chat(query)
        if resp.answer and len(resp.answer) > 20:
            return resp.answer, resp.cited_run_ids
    except Exception:
        pass

    logs = load_run_logs()
    if not logs:
        return "No pipeline run data available yet. Run a pipeline first.", []

    q = query.lower()
    total = len(logs)
    ok    = sum(1 for r in logs if r.get("status") == "success")
    err   = sum(1 for r in logs if r.get("status") == "error")

    # ── success rate ──
    if any(w in q for w in ["success rate", "pass", "overall", "how many success"]):
        rate = ok / total * 100 if total else 0
        cited = [r.get("run_id", "")[:8] for r in logs if r.get("status") == "success"][:5]
        return (
            f"**Overall success rate: {rate:.1f}%** ({ok} succeeded, {err} failed out of {total} total runs).\n\n"
            f"Sources: {', '.join(sorted({r.get('source_name','?') for r in logs if r.get('status')=='success'}))}"
        ), cited

    # ── DQ delta / improvement ──
    if any(w in q for w in ["dq", "quality", "delta", "improv", "score"]):
        deltas = [(r.get("source_name", "?"), r.get("dq_delta") or 0, r.get("dq_score_pre") or 0, r.get("dq_score_post") or 0)
                  for r in logs if r.get("dq_delta") is not None]
        if deltas:
            avg_d = sum(d for _, d, _, _ in deltas) / len(deltas)
            best = max(deltas, key=lambda x: x[1])
            worst = min(deltas, key=lambda x: x[1])
            return (
                f"**Average DQ improvement: {avg_d:+.2f} pts** across {len(deltas)} runs.\n\n"
                f"🟢 Best improvement: **{best[0]}** ({best[2]:.1f} → {best[3]:.1f}, Δ={best[1]:+.2f})\n\n"
                f"🔴 Worst: **{worst[0]}** ({worst[2]:.1f} → {worst[3]:.1f}, Δ={worst[1]:+.2f})\n\n"
                f"High DQ delta means the pipeline cleaned and enriched the data significantly."
            ), []

    # ── quarantine ──
    if any(w in q for w in ["quarantin", "flagged", "rejected"]):
        by_src: dict[str, int] = {}
        for r in logs:
            src = r.get("source_name", r.get("source", "unknown"))
            by_src[src] = by_src.get(src, 0) + (r.get("rows_quarantined") or 0)
        total_q = sum(by_src.values())
        total_r = sum((r.get("rows_in") or 0) for r in logs)
        qrate = total_q / total_r * 100 if total_r else 0
        if by_src:
            top = max(by_src, key=lambda k: by_src[k])
            rows_sorted = sorted(by_src.items(), key=lambda x: x[1], reverse=True)
            breakdown = "\n".join(f"  • {s}: {v:,}" for s, v in rows_sorted[:5])
            return (
                f"**Total quarantined: {total_q:,} rows** ({qrate:.2f}% of all input).\n\n"
                f"Source with most quarantined rows: **{top}** ({by_src[top]:,})\n\n"
                f"Breakdown:\n{breakdown}\n\n"
                f"Quarantine reasons include: null key columns, schema violations, duplicate records."
            ), []

    # ── enrichment / LLM / tiers ──
    if any(w in q for w in ["enrich", "llm", "s3", "s1", "s2", "tier", "deterministic"]):
        s1 = sum((r.get("enrichment_stats") or {}).get("deterministic", 0) for r in logs)
        s2 = sum((r.get("enrichment_stats") or {}).get("embedding", 0) for r in logs)
        s3 = sum((r.get("enrichment_stats") or {}).get("llm", 0) for r in logs)
        grand = s1 + s2 + s3 or 1
        return (
            f"**Total enrichment across all runs: {grand:,} resolutions**\n\n"
            f"🟢 S1 Deterministic (regex/keyword): **{s1:,}** ({s1/grand*100:.1f}%)\n\n"
            f"🔵 S2 KNN Corpus (FAISS similarity): **{s2:,}** ({s2/grand*100:.1f}%)\n\n"
            f"🟡 S3 RAG-LLM (Claude/LLM-assisted): **{s3:,}** ({s3/grand*100:.1f}%)\n\n"
            f"S3 only fires when S1 and S2 can't resolve — keeps LLM cost minimal."
        ), []

    # ── cost ──
    if any(w in q for w in ["cost", "usd", "spend", "money", "expensive"]):
        from src.ui.utils.api_client import prom_scalar
        try:
            total_cost = prom_scalar('sum(etl_llm_cost_usd_total)') or 0.0
            return (
                f"**Total LLM cost: ${total_cost:.4f} USD** across all pipeline runs.\n\n"
                f"Cost comes from S3 RAG-LLM enrichment (Claude Haiku). "
                f"S1 and S2 are free. The architecture is designed to minimize S3 calls."
            ), []
        except Exception:
            return "Cost data not available — Prometheus may be offline.", []

    # ── errors / failures ──
    if any(w in q for w in ["error", "fail", "broken", "crash"]):
        errors = [r for r in logs if r.get("status") == "error"]
        if errors:
            srcs = sorted({r.get("source_name", "?") for r in errors})
            err_details = "\n".join(
                f"  • {r.get('source_name','?')} — {str(r.get('error','unknown'))[:60]}"
                for r in errors[:5]
            )
            cited = [r.get("run_id", "")[:8] for r in errors[:5]]
            return (
                f"**{len(errors)} failed run(s)** out of {total} total.\n\n"
                f"Sources affected: {', '.join(srcs)}\n\n"
                f"Details:\n{err_details}"
            ), cited
        return f"No failed runs found. All {total} recorded runs completed successfully.", []

    # ── source breakdown ──
    if any(w in q for w in ["source", "which source", "breakdown"]):
        by_src: dict[str, dict] = {}
        for r in logs:
            src = r.get("source_name", "unknown")
            if src not in by_src:
                by_src[src] = {"runs": 0, "rows": 0, "ok": 0}
            by_src[src]["runs"] += 1
            by_src[src]["rows"] += (r.get("rows_in") or 0)
            if r.get("status") == "success":
                by_src[src]["ok"] += 1
        lines = "\n".join(
            f"  • **{s}**: {v['runs']} runs, {v['rows']:,} rows, {v['ok']/v['runs']*100:.0f}% success"
            for s, v in sorted(by_src.items(), key=lambda x: x[1]["rows"], reverse=True)
        )
        return f"**Pipeline runs by source:**\n\n{lines}", []

    # ── default summary ──
    total_rows = sum((r.get("rows_in") or 0) for r in logs)
    total_out  = sum((r.get("rows_out") or 0) for r in logs)
    return (
        f"**MIP Pipeline Summary** — {total} runs recorded\n\n"
        f"✅ Success: {ok} | ❌ Failed: {err}\n\n"
        f"📦 Total rows processed: {total_rows:,} in → {total_out:,} out\n\n"
        f"Try asking: *success rate*, *DQ improvement*, *quarantine breakdown*, "
        f"*enrichment tiers*, *LLM cost*, *failed runs*, or *source breakdown*."
    ), []
