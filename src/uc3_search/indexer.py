"""
UC3 Hybrid Search — OpenSearch Index Builder

Planned implementation:
- Index enriched product catalog into OpenSearch
- Two fields per product: BM25 text field + dense vector field
- BM25 over: clean_name + primary_category + dietary_tags
- Dense vector over: enriched text embedding (sentence-transformers)

Dependencies: opensearch-py, sentence-transformers
"""

from __future__ import annotations

import pandas as pd


class ProductIndexer:
    """Builds and manages the OpenSearch product index."""

    def __init__(self, opensearch_host: str = "localhost", port: int = 9200):
        raise NotImplementedError("UC3 — planned for next sprint")

    def create_index(self, index_name: str = "products") -> None:
        """Create the OpenSearch index with BM25 + dense vector mappings."""
        raise NotImplementedError

    def index_products(self, df: pd.DataFrame) -> int:
        """
        Index enriched products into OpenSearch.

        Returns number of documents indexed.
        """
        raise NotImplementedError

    def delete_index(self, index_name: str = "products") -> None:
        """Delete the product index."""
        raise NotImplementedError
