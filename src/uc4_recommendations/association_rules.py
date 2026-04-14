"""
UC4 Recommendations — Association Rule Mining

Planned implementation:
- Instacart 37.3M transaction rows → Apriori/FP-Growth via mlxtend
- Compute support, confidence, lift per product pair
- Runs on deduplicated product IDs (dependency on UC1)
- Key insight: signal consolidation after dedup improves lift 3-4x

Dependencies: mlxtend, pandas
"""

from __future__ import annotations

import pandas as pd


class AssociationRuleMiner:
    """Mine co-purchase association rules from transaction data."""

    def __init__(self, transactions_path: str):
        raise NotImplementedError("UC4 — planned for next sprint")

    def mine_rules(self, min_support: float = 0.01, min_confidence: float = 0.1) -> pd.DataFrame:
        """
        Run Apriori algorithm on transaction baskets.

        Returns DataFrame with columns: antecedent, consequent, support, confidence, lift
        """
        raise NotImplementedError

    def get_recommendations(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        Get co-purchase recommendations for a product.

        Returns [{"product": ..., "confidence": ..., "lift": ...}]
        """
        raise NotImplementedError

    def compare_raw_vs_enriched(self, product_id: str) -> dict:
        """
        Compare recommendation quality before/after enrichment.

        Returns {"raw": [...], "enriched": [...], "lift_improvement": float}
        """
        raise NotImplementedError
