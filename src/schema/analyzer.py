"""Schema analysis utilities — profile DataFrames and diff against unified schema."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"
UNIFIED_SCHEMA_PATH = CONFIG_DIR / "unified_schema.json"


def profile_dataframe(df: pd.DataFrame, sample_size: int = 5) -> dict:
    """
    Profile a DataFrame's schema.

    Returns {column_name: {dtype, null_rate, unique_count, sample_values}}.
    """
    profile = {}
    for col in df.columns:
        non_null = df[col].dropna()
        samples = non_null.head(sample_size).astype(str).tolist() if len(non_null) > 0 else []
        profile[col] = {
            "dtype": str(df[col].dtype),
            "null_rate": round(float(df[col].isna().mean()), 4),
            "unique_count": int(df[col].nunique()),
            "sample_values": samples,
        }
    return profile


def load_unified_schema() -> dict | None:
    """Load unified schema from config. Returns None if it doesn't exist."""
    if not UNIFIED_SCHEMA_PATH.exists():
        return None
    with open(UNIFIED_SCHEMA_PATH) as f:
        return json.load(f)


def save_unified_schema(schema: dict) -> None:
    """Save unified schema to config."""
    UNIFIED_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(UNIFIED_SCHEMA_PATH, "w") as f:
        json.dump(schema, f, indent=2)


def derive_unified_schema_from_source(
    df: pd.DataFrame,
    column_mapping: dict[str, str],
    domain: str,
) -> dict:
    """
    Derive a unified schema from the first data source.

    column_mapping: {source_col -> unified_col}
    Returns a schema dict compatible with config/unified_schema.json format.
    """
    columns = {}
    for source_col, unified_col in column_mapping.items():
        dtype = str(df[source_col].dtype)
        # Map pandas dtypes to schema types
        if "int" in dtype:
            schema_type = "integer"
        elif "float" in dtype:
            schema_type = "float"
        elif "bool" in dtype:
            schema_type = "boolean"
        else:
            schema_type = "string"

        null_rate = float(df[source_col].isna().mean())
        columns[unified_col] = {
            "type": schema_type,
            "required": null_rate < 0.5,
        }

    # Add standard enrichment columns
    for enrich_col in ["allergens", "primary_category", "dietary_tags", "is_organic"]:
        if enrich_col not in columns:
            columns[enrich_col] = {
                "type": "boolean" if enrich_col == "is_organic" else "string",
                "required": False,
                "enrichment": True,
            }

    # Add computed columns
    for computed_col in ["dq_score_pre", "dq_score_post", "dq_delta"]:
        columns[computed_col] = {
            "type": "float",
            "required": True,
            "computed": True,
        }

    return {
        "columns": columns,
        "dq_weights": {
            "completeness": 0.4,
            "freshness": 0.35,
            "ingredient_richness": 0.25,
        },
    }


def compute_schema_diff(
    source_profile: dict,
    unified_schema: dict,
) -> tuple[dict, list[dict]]:
    """
    Compute the diff between a source profile and the unified schema.

    Returns:
        column_mapping: {source_col -> unified_col} for direct matches
        gaps: list of gap dicts for columns that need transformation
    """
    unified_cols = unified_schema.get("columns", {})

    # Columns that are computed or enrichment-only don't need source mapping
    mappable_cols = {
        name: spec
        for name, spec in unified_cols.items()
        if not spec.get("computed") and not spec.get("enrichment")
    }

    column_mapping = {}
    gaps = []

    # Try exact name matches first
    source_cols_remaining = set(source_profile.keys())
    target_cols_remaining = set(mappable_cols.keys())

    for src_col in list(source_cols_remaining):
        if src_col in target_cols_remaining:
            column_mapping[src_col] = src_col
            source_cols_remaining.discard(src_col)
            target_cols_remaining.discard(src_col)

    # Remaining target cols that have no source match are gaps
    for target_col in target_cols_remaining:
        target_spec = mappable_cols[target_col]
        gaps.append({
            "target_column": target_col,
            "target_type": target_spec["type"],
            "source_column": None,
            "source_type": None,
            "action": "ADD",
            "sample_values": [],
        })

    return column_mapping, gaps
