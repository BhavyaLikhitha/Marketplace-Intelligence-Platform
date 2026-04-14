"""
UC2 Observability Layer — Anomaly Detection

Planned implementation:
- Isolation Forest on per-run pipeline metrics
- Detects: null rate spikes, row count deviations, DQ score shifts,
  LLM confidence degradation, duplicate rate anomalies

Dependencies: scikit-learn (IsolationForest), pipeline metrics time-series
"""

from __future__ import annotations

import pandas as pd


class AnomalyDetector:
    """Isolation Forest based anomaly detection on pipeline run metrics."""

    def __init__(self, metrics_history: pd.DataFrame):
        raise NotImplementedError("UC2 — planned for next sprint")

    def fit(self) -> None:
        """Train the Isolation Forest on historical run metrics."""
        raise NotImplementedError

    def detect(self, current_run_metrics: dict) -> list[dict]:
        """
        Detect anomalies in the current run.

        Returns list of anomaly alerts:
        [{"metric": "null_rate_brand", "value": 0.40, "baseline": 0.02, "severity": "high"}]
        """
        raise NotImplementedError

    def detect_null_spike(self, field: str, current_rate: float) -> dict | None:
        """Check if null rate for a field spiked beyond threshold."""
        raise NotImplementedError

    def detect_row_count_deviation(self, expected: int, actual: int) -> dict | None:
        """Check if row count deviates significantly from baseline."""
        raise NotImplementedError

    def detect_dq_shift(self, current_distribution: pd.Series) -> dict | None:
        """Check if DQ score distribution shifted significantly."""
        raise NotImplementedError
