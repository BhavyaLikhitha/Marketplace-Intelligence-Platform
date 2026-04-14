"""
UC4 Recommendations — Product Recommender

Planned implementation:
- "Customers who bought this also bought" — direct co-purchase associations
- "You might also like" — cross-category affinity via graph traversal
- Side-by-side demo: raw (noisy, low lift) vs enriched (consolidated, high lift)

Dependencies: association_rules.py, graph_store.py
"""

from __future__ import annotations


class ProductRecommender:
    """Unified recommender combining association rules and graph traversal."""

    def __init__(self, rules_path: str, graph_uri: str | None = None):
        raise NotImplementedError("UC4 — planned for next sprint")

    def also_bought(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        "Customers who bought this also bought:" — direct co-purchase.

        Returns [{"product_name": ..., "confidence": ..., "lift": ...}]
        """
        raise NotImplementedError

    def you_might_like(self, product_id: str, top_k: int = 5) -> list[dict]:
        """
        "You might also like:" — cross-category affinity.

        Returns [{"product_name": ..., "category": ..., "affinity_score": ...}]
        """
        raise NotImplementedError

    def demo_comparison(self, product_id: str) -> dict:
        """
        Generate side-by-side comparison for demo.

        Returns {
            "raw_recommendations": [...],
            "enriched_recommendations": [...],
            "lift_improvement": float,
            "signal_consolidation": {"raw_ids": int, "enriched_ids": int}
        }
        """
        raise NotImplementedError
