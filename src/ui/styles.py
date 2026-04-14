"""CSS styles for the ETL pipeline Streamlit app."""

GLOBAL_CSS = """
<style>
    /* ── Base overrides ─────────────────────────────────── */
    .stApp { background-color: #0e1117; }

    /* ── Step indicator bar ──────────────────────────────── */
    .step-bar {
        display: flex; gap: 0; margin: 1.5rem 0 2rem 0;
        border-radius: 8px; overflow: hidden;
        border: 1px solid #1e2530;
    }
    .step-item {
        flex: 1; padding: 12px 16px; text-align: center;
        background: #151b23; color: #6b7685;
        font-size: 0.82rem; font-weight: 500;
        border-right: 1px solid #1e2530;
        transition: all 0.2s ease;
    }
    .step-item:last-child { border-right: none; }
    .step-item.active {
        background: #1a3a5c; color: #58a6ff;
        box-shadow: inset 0 -2px 0 #58a6ff;
    }
    .step-item.done {
        background: #122b1e; color: #3fb950;
    }
    .step-item .step-num {
        display: inline-block; width: 22px; height: 22px;
        line-height: 22px; border-radius: 50%;
        background: #21262d; color: #6b7685;
        font-size: 0.72rem; margin-right: 6px;
    }
    .step-item.active .step-num { background: #1f4e79; color: #58a6ff; }
    .step-item.done .step-num { background: #1b4332; color: #3fb950; }

    /* ── Section headers ────────────────────────────────── */
    .section-header {
        font-size: 1.1rem; font-weight: 600;
        color: #e6edf3; margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #21262d;
    }

    /* ── Badges ─────────────────────────────────────────── */
    .badge {
        display: inline-block; padding: 2px 8px;
        border-radius: 12px; font-size: 0.72rem;
        font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .badge-map { background: #1a3a5c; color: #58a6ff; }
    .badge-add { background: #3b2e00; color: #d29922; }
    .badge-drop { background: #3d1f1f; color: #f85149; }
    .badge-new { background: #1b4332; color: #3fb950; }
    .badge-hit { background: #1b4332; color: #3fb950; }
    .badge-miss { background: #3d1f1f; color: #f85149; }
    .badge-pass { background: #1b4332; color: #3fb950; }
    .badge-fail { background: #3d1f1f; color: #f85149; }

    /* ── Schema delta table ─────────────────────────────── */
    .schema-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.84rem; margin: 0.5rem 0;
    }
    .schema-table th {
        background: #161b22; color: #8b949e;
        padding: 10px 14px; text-align: left;
        font-weight: 600; font-size: 0.76rem;
        text-transform: uppercase; letter-spacing: 0.04em;
        border-bottom: 2px solid #21262d;
    }
    .schema-table td {
        padding: 9px 14px; color: #c9d1d9;
        border-bottom: 1px solid #1a1f28;
    }
    .schema-table tr:hover td { background: #161b22; }
    .schema-table .col-source { color: #f0883e; font-family: monospace; }
    .schema-table .col-unified { color: #58a6ff; font-family: monospace; }
    .schema-table .col-type { color: #8b949e; font-family: monospace; font-size: 0.78rem; }
    .schema-table .col-null { font-family: monospace; }
    .schema-table .null-low { color: #3fb950; }
    .schema-table .null-mid { color: #d29922; }
    .schema-table .null-high { color: #f85149; }

    /* ── Metric cards ───────────────────────────────────── */
    .metric-row { display: flex; gap: 16px; margin: 1rem 0; }
    .metric-card {
        flex: 1; background: #151b23;
        border: 1px solid #21262d; border-radius: 10px;
        padding: 20px 24px; text-align: center;
    }
    .metric-card .metric-label {
        font-size: 0.76rem; color: #8b949e;
        text-transform: uppercase; letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-card .metric-value {
        font-size: 2rem; font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-card .metric-sub {
        font-size: 0.78rem; color: #6b7685;
        margin-top: 4px;
    }
    .val-good { color: #3fb950; }
    .val-warn { color: #d29922; }
    .val-bad { color: #f85149; }
    .val-neutral { color: #58a6ff; }

    /* ── Code review block ──────────────────────────────── */
    .code-review {
        background: #0d1117; border: 1px solid #21262d;
        border-radius: 8px; margin: 0.75rem 0;
        overflow: hidden;
    }
    .code-review-header {
        background: #161b22; padding: 10px 16px;
        display: flex; justify-content: space-between;
        align-items: center; border-bottom: 1px solid #21262d;
    }
    .code-review-header .fn-name {
        font-family: monospace; color: #d2a8ff;
        font-size: 0.88rem; font-weight: 600;
    }
    .code-review pre {
        margin: 0; padding: 16px;
        background: #0d1117; color: #c9d1d9;
        font-size: 0.82rem; line-height: 1.5;
        overflow-x: auto; font-family: 'JetBrains Mono', monospace;
    }
    .code-review .validation-bar {
        background: #161b22; padding: 10px 16px;
        border-top: 1px solid #21262d;
        font-size: 0.8rem; color: #8b949e;
    }

    /* ── Sample I/O table ───────────────────────────────── */
    .io-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .io-table th {
        background: #161b22; color: #8b949e;
        padding: 8px 12px; text-align: left;
        font-size: 0.74rem; text-transform: uppercase;
        border-bottom: 1px solid #21262d;
    }
    .io-table td {
        padding: 7px 12px; color: #c9d1d9;
        border-bottom: 1px solid #1a1f28;
        font-family: monospace;
    }
    .io-table .val-in { color: #f0883e; }
    .io-table .val-out { color: #3fb950; }

    /* ── Block waterfall ────────────────────────────────── */
    .waterfall { margin: 1rem 0; }
    .waterfall-row {
        display: flex; align-items: center;
        margin: 4px 0; font-size: 0.8rem;
    }
    .waterfall-label {
        width: 180px; color: #8b949e;
        font-family: monospace; font-size: 0.78rem;
        text-align: right; padding-right: 12px;
        flex-shrink: 0;
    }
    .waterfall-bar-wrap { flex: 1; display: flex; align-items: center; gap: 8px; }
    .waterfall-bar {
        height: 22px; border-radius: 3px;
        background: #1a3a5c; min-width: 4px;
        transition: width 0.4s ease;
    }
    .waterfall-bar.loss { background: #3d1f1f; }
    .waterfall-count {
        color: #6b7685; font-family: monospace;
        font-size: 0.76rem; white-space: nowrap;
    }

    /* ── Quarantine table ───────────────────────────────── */
    .quarantine-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .quarantine-table th {
        background: #2d1515; color: #f85149;
        padding: 10px 14px; text-align: left;
        font-size: 0.76rem; text-transform: uppercase;
        border-bottom: 2px solid #3d1f1f;
    }
    .quarantine-table td {
        padding: 8px 14px; color: #c9d1d9;
        border-bottom: 1px solid #1a1f28;
    }
    .quarantine-table .reason { color: #f0883e; font-size: 0.78rem; }

    /* ── Enrichment breakdown ───────────────────────────── */
    .enrich-breakdown { margin: 1rem 0; }
    .enrich-row {
        display: flex; align-items: center;
        margin: 6px 0; font-size: 0.82rem;
    }
    .enrich-tier {
        width: 140px; color: #8b949e;
        font-size: 0.78rem; flex-shrink: 0;
    }
    .enrich-bar-wrap { flex: 1; display: flex; align-items: center; gap: 8px; }
    .enrich-bar {
        height: 20px; border-radius: 3px; min-width: 2px;
        transition: width 0.4s ease;
    }
    .enrich-bar.tier-1 { background: #1b4332; }
    .enrich-bar.tier-2 { background: #1a3a5c; }
    .enrich-bar.tier-3 { background: #3b2e00; }
    .enrich-bar.tier-4 { background: #5c1a1a; }
    .enrich-count {
        color: #6b7685; font-family: monospace; font-size: 0.76rem;
    }

    /* ── Source profile table ───────────────────────────── */
    .profile-table {
        width: 100%; border-collapse: collapse;
        font-size: 0.82rem; margin: 0.5rem 0;
    }
    .profile-table th {
        background: #161b22; color: #8b949e;
        padding: 8px 12px; text-align: left;
        font-size: 0.74rem; text-transform: uppercase;
        border-bottom: 2px solid #21262d;
    }
    .profile-table td {
        padding: 7px 12px; color: #c9d1d9;
        border-bottom: 1px solid #1a1f28;
    }
    .profile-table .col-name { font-family: monospace; color: #f0883e; }
    .profile-table .sample { color: #6b7685; font-size: 0.76rem; max-width: 300px; overflow: hidden; text-overflow: ellipsis; }

    /* ── Pipeline Remembered banner ─────────────────────── */
    .remembered-banner {
        background: #122b1e;
        border: 1px solid #3fb950;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 0.5rem 0 1rem;
    }
    .remembered-title {
        color: #3fb950;
        font-weight: 600;
        font-size: 1rem;
        margin-bottom: 8px;
    }
    .remembered-list {
        color: #e6edf3;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        margin: 0 0 10px 1.2rem;
        padding: 0;
    }
    .remembered-sub {
        color: #8b949e;
        font-size: 0.82rem;
    }
</style>
"""
