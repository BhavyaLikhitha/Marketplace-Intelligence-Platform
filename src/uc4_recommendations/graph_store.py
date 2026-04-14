"""
UC4 Recommendations — Neo4j Graph Store

Planned implementation:
- Products, brands, categories as nodes
- Co-purchase relationships as edges with lift score as weight
- Shortest path between distant categories = non-obvious recommendation
- Graph visualization for demo

Dependencies: neo4j, py2neo
"""

from __future__ import annotations

import pandas as pd


class ProductGraph:
    """Neo4j graph store for product relationships."""

    def __init__(self, uri: str = "bolt://localhost:7687", auth: tuple = ("neo4j", "password")):
        raise NotImplementedError("UC4 — planned for next sprint")

    def load_products(self, df: pd.DataFrame) -> int:
        """Load product nodes into Neo4j. Returns count of nodes created."""
        raise NotImplementedError

    def load_relationships(self, rules_df: pd.DataFrame) -> int:
        """Load co-purchase edges with lift weights. Returns count of edges created."""
        raise NotImplementedError

    def find_path(self, product_a: str, product_b: str) -> list[dict]:
        """Find shortest path between two products through the graph."""
        raise NotImplementedError

    def cross_category_recommendations(self, product_id: str, max_hops: int = 2) -> list[dict]:
        """Find cross-category recommendations via graph traversal."""
        raise NotImplementedError
