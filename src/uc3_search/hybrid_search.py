"""
UC3 Hybrid Search — BM25 + Semantic Search with RRF

Planned implementation:
- BM25 keyword retrieval from OpenSearch
- Semantic retrieval via dense vector similarity
- Reciprocal Rank Fusion (RRF) to merge both ranked lists
- Results include: product name, brand, category, DQ score, allergens

Dependencies: opensearch-py, sentence-transformers
"""

from __future__ import annotations


class HybridSearch:
    """BM25 + semantic hybrid search with Reciprocal Rank Fusion."""

    def __init__(self, opensearch_host: str = "localhost", port: int = 9200):
        raise NotImplementedError("UC3 — planned for next sprint")

    def search(self, query: str, top_k: int = 10) -> list[dict]:
        """
        Execute hybrid search.

        Returns ranked list of product results:
        [{"product_name": ..., "brand": ..., "category": ..., "score": ..., "allergens": ...}]
        """
        raise NotImplementedError

    def bm25_search(self, query: str, top_k: int = 50) -> list[dict]:
        """BM25 keyword search against OpenSearch."""
        raise NotImplementedError

    def semantic_search(self, query: str, top_k: int = 50) -> list[dict]:
        """Dense vector similarity search."""
        raise NotImplementedError

    def reciprocal_rank_fusion(
        self, bm25_results: list[dict], semantic_results: list[dict], k: int = 60
    ) -> list[dict]:
        """Merge two ranked lists using RRF."""
        raise NotImplementedError
