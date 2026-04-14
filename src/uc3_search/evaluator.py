"""
UC3 Hybrid Search — LLM-as-Judge Evaluation

Planned implementation:
- 300-500 query/result pairs from Amazon ESCI benchmark
- Claude judges relevance: Exact / Substitute / Complement / Irrelevant
- nDCG and MRR computed before and after enrichment
- The delta proves UC1 improved search quality

Dependencies: langchain, anthropic, numpy
"""

from __future__ import annotations

import pandas as pd


class SearchEvaluator:
    """LLM-as-Judge evaluation for search quality (nDCG, MRR)."""

    def __init__(self, esci_data_path: str):
        raise NotImplementedError("UC3 — planned for next sprint")

    def evaluate(self, search_results: list[dict]) -> dict:
        """
        Run full evaluation pipeline.

        Returns {"ndcg": float, "mrr": float, "relevance_distribution": dict}
        """
        raise NotImplementedError

    def judge_relevance(self, query: str, product: dict) -> str:
        """
        Use Claude to judge relevance.

        Returns one of: "exact", "substitute", "complement", "irrelevant"
        """
        raise NotImplementedError

    def compute_ndcg(self, relevance_scores: list[int], k: int = 10) -> float:
        """Compute normalized Discounted Cumulative Gain at k."""
        raise NotImplementedError

    def compute_mrr(self, relevance_scores: list[int]) -> float:
        """Compute Mean Reciprocal Rank."""
        raise NotImplementedError
