"""Data quality scoring — pre and post enrichment."""

from __future__ import annotations

import logging

import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)


def compute_dq_score(
    df: pd.DataFrame,
    weights: dict | None = None,
) -> pd.Series:
    """
    Compute a DQ score per row.

    Score = Completeness * w1 + Freshness * w2 + IngredientRichness * w3
    """
    weights = weights or {"completeness": 0.4, "freshness": 0.35, "ingredient_richness": 0.25}

    # Ignore computed/meta columns
    skip = {"dq_score_pre", "dq_score_post", "dq_delta",
            "duplicate_group_id", "canonical"}
    data_cols = [c for c in df.columns if c not in skip]

    # Completeness: fraction of non-null values
    completeness = df[data_cols].notna().mean(axis=1)

    # Freshness proxy: if published_date exists
    if "published_date" in df.columns:
        dates = pd.to_datetime(df["published_date"], errors="coerce")
        if dates.notna().any():
            min_d, max_d = dates.min(), dates.max()
            freshness = ((dates - min_d) / (max_d - min_d)).fillna(0.5) if min_d != max_d else 1.0
        else:
            freshness = 0.5
    else:
        freshness = 0.5

    # Ingredient richness
    if "ingredients" in df.columns:
        lengths = df["ingredients"].fillna("").astype(str).str.len()
        max_len = lengths.max()
        richness = lengths / max_len if max_len > 0 else 0
    else:
        richness = 0

    score = (
        completeness * weights["completeness"]
        + freshness * weights["freshness"]
        + richness * weights["ingredient_richness"]
    )
    return (score * 100).round(2)


class DQScorePreBlock(Block):
    name = "dq_score_pre"
    domain = "all"

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        weights = (config or {}).get("dq_weights")
        df["dq_score_pre"] = compute_dq_score(df, weights)
        mean_score = df["dq_score_pre"].mean()
        logger.info(f"DQ Score (pre): mean={mean_score:.1f}%, min={df['dq_score_pre'].min():.1f}%, max={df['dq_score_pre'].max():.1f}%")
        return df


class DQScorePostBlock(Block):
    name = "dq_score_post"
    domain = "all"

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        weights = (config or {}).get("dq_weights")
        df["dq_score_post"] = compute_dq_score(df, weights)
        if "dq_score_pre" in df.columns:
            df["dq_delta"] = (df["dq_score_post"] - df["dq_score_pre"]).round(2)
            mean_delta = df["dq_delta"].mean()
            logger.info(f"DQ Score (post): mean={df['dq_score_post'].mean():.1f}%, delta={mean_delta:+.1f}%")
        return df
