"""
UC2 Observability Layer — Pipeline Dashboard

Planned implementation:
- Streamlit dashboard consuming pipeline audit logs
- Per-run metrics: block execution trace, null rates, DQ distributions
- Time-series trends across runs
- Cost tracking (LLM tokens, cache hit rates)

Dependencies: streamlit, plotly, pipeline audit_log output
"""

from __future__ import annotations


class PipelineDashboard:
    """Streamlit-based dashboard for UC1 pipeline observability."""

    def __init__(self, audit_log_dir: str):
        raise NotImplementedError("UC2 — planned for next sprint")

    def render(self) -> None:
        """Render the full dashboard with all diagnostic panels."""
        raise NotImplementedError

    def render_block_trace(self) -> None:
        """Show which blocks ran, in what order, rows in/out per block."""
        raise NotImplementedError

    def render_null_rates(self) -> None:
        """Show null rate per field before and after each block."""
        raise NotImplementedError

    def render_dq_distribution(self) -> None:
        """Histogram of DQ scores pre vs post enrichment, per data source."""
        raise NotImplementedError

    def render_cost_tracking(self) -> None:
        """LLM calls per run, tokens in/out, cost per run, cache hit rate."""
        raise NotImplementedError
