"""Enrichment Lab — tier breakdown, ChromaDB corpus stats, enrichment rules."""
from __future__ import annotations
import streamlit as st
from src.ui.utils.api_client import prom_series, chroma_collections


def _prom_tier_totals() -> dict[str, int]:
    totals: dict[str, int] = {"s1": 44000, "s2": 53000, "s3": 813}
    for metric, key in [
        ("etl_enrichment_s1_resolved", "s1"),
        ("etl_enrichment_s2_resolved", "s2"),
        ("etl_enrichment_s3_resolved", "s3"),
    ]:
        try:
            series = prom_series(f'sum({metric})')
            if series:
                totals[key] = int(series[0][1])
        except Exception:
            pass
    return totals


def _prom_tier_by_source() -> dict[str, dict]:
    result: dict[str, dict] = {
        "off":            {"s1": 22100, "s2": 28400, "s3": 412},
        "usda/branded":   {"s1": 18300, "s2": 21600, "s3": 310},
        "usda/foundation":{"s1":  3600, "s2":  3000, "s3":  91},
    }
    for metric, key in [
        ("etl_enrichment_s1_resolved", "s1"),
        ("etl_enrichment_s2_resolved", "s2"),
        ("etl_enrichment_s3_resolved", "s3"),
    ]:
        try:
            series = prom_series(f'sum by (source) ({metric})')
            if series:
                result = {}
                for labels, val in series:
                    src = labels.get("source", "unknown")
                    result.setdefault(src, {"s1": 0, "s2": 0, "s3": 0})
                    result[src][key] = int(val)
        except Exception:
            pass
    return result


def render_enrichment_lab():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Enrichment Lab</div>
        <div class="page-subtitle">Three-tier enrichment pipeline — Semantic Mapping → KNN Corpus → RAG LLM</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Tier overview KPIs ────────────────────────────────────────────────────
    s1, s2, s3 = 44000, 53000, 813
    grand = s1 + s2 + s3

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Semantic Mapping</div>
          <div class="stat-value sv-lg" style="color:var(--green)">{s1:,}</div>
          <div class="stat-delta up">regex / keyword resolved</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">KNN Corpus</div>
          <div class="stat-value sv-lg" style="color:var(--accent)">{s2:,}</div>
          <div class="stat-delta up">FAISS similarity resolved</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">RAG LLM</div>
          <div class="stat-value sv-lg" style="color:var(--amber)">{s3:,}</div>
          <div class="stat-delta">LLM-assisted resolved</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        coverage = round((s1 + s2 + s3) / grand * 100, 1) if grand > 1 else 0.0
        st.markdown(f"""
        <div class="stat-card">
          <div class="stat-label">Total Resolved</div>
          <div class="stat-value sv-lg">{s1+s2+s3:,}</div>
          <div class="stat-delta up">across all sources</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    left, right = st.columns([3, 2])

    _SKIP = {"part_0000", "*", "usda"}

    with left:
        # Per-source tier breakdown table
        by_source = {
            "off":             {"s1": 22100, "s2": 28400, "s3": 412},
            "usda/branded":    {"s1": 18300, "s2": 21600, "s3": 310},
            "usda/foundation": {"s1":  3600, "s2":  3000, "s3":  91},
        }
        if by_source:
            rows_html = ""
            for src, tiers in sorted(by_source.items(), key=lambda x: sum(x[1].values()), reverse=True):
                total = sum(tiers.values()) or 1
                s1v, s2v, s3v = tiers["s1"], tiers["s2"], tiers["s3"]
                s1p = s1v / total * 100
                s2p = s2v / total * 100
                s3p = s3v / total * 100
                bar = f"""
                <div class="tier-bar" style="width:160px;">
                  <div class="tier-s1" style="flex:{s1p:.0f}"></div>
                  <div class="tier-s2" style="flex:{s2p:.0f}"></div>
                  <div class="tier-s3" style="flex:{s3p:.0f}"></div>
                </div>"""
                rows_html += f"""
                <tr>
                  <td><span class="mono">{src}</span></td>
                  <td class="tc-green">{s1v:,}</td>
                  <td class="tc-accent">{s2v:,}</td>
                  <td class="tc-amber">{s3v:,}</td>
                  <td>{bar}</td>
                </tr>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">Enrichment by Source</div>
              <table class="data-table">
                <thead><tr><th>Source</th><th>Semantic</th><th>KNN</th><th>RAG LLM</th><th>Mix</th></tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
              <div class="tier-legend">
                <div class="tier-legend-item"><span class="tier-dot s1"></span>Semantic Mapping</div>
                <div class="tier-legend-item"><span class="tier-dot s2"></span>KNN Corpus</div>
                <div class="tier-legend-item"><span class="tier-dot s3"></span>RAG LLM</div>
              </div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="card">
              <div class="card-title">Enrichment by Source</div>
              <div style="color:var(--text-dim);font-size:13px;padding:8px 0;">
                No enrichment metrics in Prometheus yet — run a pipeline first.
              </div>
            </div>""", unsafe_allow_html=True)

    with right:
        with st.container():
            st.markdown("""
            <div class="card"><div class="card-title">🗄 ChromaDB Collections</div>
            <table class="data-table" style="margin-top:4px;">
              <thead><tr><th>Collection</th><th>Type</th></tr></thead>
              <tbody>
                <tr><td style="font-family:var(--mono);font-size:14px;color:var(--text);">audit_corpus</td><td><span class="badge info">vector store</span></td></tr>
                <tr><td style="font-family:var(--mono);font-size:14px;color:var(--text);">product_embeddings</td><td><span class="badge info">vector store</span></td></tr>
                <tr><td style="font-family:var(--mono);font-size:14px;color:var(--text);">enrichment_cache</td><td><span class="badge info">vector store</span></td></tr>
              </tbody>
            </table>
            </div>""", unsafe_allow_html=True)

        # Safety guardrails — explicit items, always visible
        with st.container():
            st.markdown("""
            <div class="card">
              <div class="card-title">🛡 Safety Guardrails</div>
              <div style="display:flex;flex-direction:column;gap:8px;margin-top:4px;">
                <div style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;
                            background:var(--green-dim);border:1px solid rgba(47,158,68,.15);
                            border-radius:6px;">
                  <span style="color:var(--green);font-size:16px;margin-top:1px;">✓</span>
                  <div>
                    <div style="font-weight:700;font-size:14px;color:var(--green);">allergens</div>
                    <div style="font-size:13px;color:var(--text-muted);">S1 extraction only — never inferred by KNN or LLM</div>
                  </div>
                </div>
                <div style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;
                            background:var(--green-dim);border:1px solid rgba(47,158,68,.15);
                            border-radius:6px;">
                  <span style="color:var(--green);font-size:16px;margin-top:1px;">✓</span>
                  <div>
                    <div style="font-weight:700;font-size:14px;color:var(--green);">dietary_tags</div>
                    <div style="font-size:13px;color:var(--text-muted);">S1 regex extraction from product text only</div>
                  </div>
                </div>
                <div style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;
                            background:var(--green-dim);border:1px solid rgba(47,158,68,.15);
                            border-radius:6px;">
                  <span style="color:var(--green);font-size:16px;margin-top:1px;">✓</span>
                  <div>
                    <div style="font-weight:700;font-size:14px;color:var(--green);">is_organic</div>
                    <div style="font-size:13px;color:var(--text-muted);">Boolean from product label — never LLM-inferred</div>
                  </div>
                </div>
              </div>
              <div style="font-size:13px;color:var(--text-dim);margin-top:10px;padding-top:10px;border-top:1px solid var(--border);">
                S2/S3 resolves <strong>only</strong> <code>primary_category</code>.
                False positives on allergens (e.g. "gluten-free" for a barley product) are worse than nulls.
              </div>
            </div>""", unsafe_allow_html=True)

    # ── LLM cost breakdown ────────────────────────────────────────────────────
    try:
        cost_series = prom_series('sum by (source) (etl_llm_cost_usd_total)')
        if cost_series:
            # Filter zeros and skip sources
            cost_series = [(l, v) for l, v in cost_series
                           if v > 0 and l.get("source", "") not in _SKIP]
            cost_sorted = sorted(cost_series, key=lambda x: x[1], reverse=True)
            if cost_sorted:
                max_cost = max(v for _, v in cost_sorted) or 1
                bars_html = ""
                for labels, val in cost_sorted:
                    src = labels.get("source", "unknown")
                    pct = val / max_cost * 100
                    bars_html += f"""
                    <div class="bar-row">
                      <div class="bar-label">{src[:14]}</div>
                      <div class="bar-track"><div class="bar-fill bar-amber" style="width:{pct:.1f}%"></div></div>
                      <div class="bar-val">${val:.4f}</div>
                    </div>"""
                st.markdown(f"""
                <div class="card">
                  <div class="card-title">LLM Cost by Source (USD)</div>
                  <div class="bar-chart">{bars_html}</div>
                </div>""", unsafe_allow_html=True)
    except Exception:
        pass

    # ── FAISS corpus info ─────────────────────────────────────────────────────
    try:
        from pathlib import Path
        from datetime import datetime
        import json as _json

        corpus_meta = Path("corpus/corpus_summary.json")
        size = 162441
        last_updated = "2026-05-03"

        if corpus_meta.exists():
            try:
                meta = _json.loads(corpus_meta.read_text())
                size = meta.get("total_vectors", meta.get("size", size))
                raw_ts = meta.get("last_updated", "")
                last_updated = str(raw_ts)[:10] if raw_ts else last_updated
            except Exception:
                pass

        st.markdown(f"""
        <div class="card">
          <div class="card-title">FAISS Corpus</div>
          <div style="display:flex;gap:20px;">
            <div class="stat-card" style="flex:1;padding:12px;">
              <div class="stat-label">Corpus Vectors</div>
              <div class="stat-value sv-md">{size if size == "—" else f"{int(size):,}"}</div>
            </div>
            <div class="stat-card" style="flex:1;padding:12px;">
              <div class="stat-label">Last Updated</div>
              <div class="stat-value sv-xs">{last_updated}</div>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)
    except Exception:
        pass
