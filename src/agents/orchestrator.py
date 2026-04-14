"""Agent 1 — Orchestrator: schema analysis, gap detection, registry check."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.agents.state import PipelineState
from src.agents.prompts import SCHEMA_ANALYSIS_PROMPT, FIRST_RUN_SCHEMA_PROMPT
from src.models.llm import call_llm_json, get_orchestrator_llm
from src.schema.analyzer import (
    profile_dataframe,
    load_unified_schema,
    save_unified_schema,
    derive_unified_schema_from_source,
)
from src.registry.function_registry import FunctionRegistry

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REGISTRY_DIR = PROJECT_ROOT / "function_registry"


def load_source_node(state: PipelineState) -> dict:
    """Load CSV and compute schema profile."""
    source_path = state["source_path"]
    logger.info(f"Loading source: {source_path}")

    df = pd.read_csv(source_path)
    schema = profile_dataframe(df)

    return {
        "source_df": df,
        "source_schema": schema,
    }


def analyze_schema_node(state: PipelineState) -> dict:
    """
    Agent 1 LLM call: analyze source schema against unified schema.

    On first run (no unified schema), derives the schema from source.
    On subsequent runs, diffs against existing unified schema.
    """
    source_schema = state["source_schema"]
    domain = state.get("domain", "nutrition")
    model = get_orchestrator_llm()

    unified = load_unified_schema()

    if unified is None:
        # First run — derive unified schema
        logger.info("No unified schema found — first run, deriving from source")
        result = call_llm_json(
            model=model,
            messages=[{
                "role": "user",
                "content": FIRST_RUN_SCHEMA_PROMPT.format(
                    source_schema=json.dumps(source_schema, indent=2),
                    domain=domain,
                ),
            }],
        )

        column_mapping = result.get("column_mapping", {})
        gaps = result.get("gaps", [])

        # Derive and save unified schema
        df = state["source_df"]
        unified = derive_unified_schema_from_source(df, column_mapping, domain)
        save_unified_schema(unified)
        logger.info(f"Unified schema derived and saved with {len(unified['columns'])} columns")

        return {
            "unified_schema": unified,
            "unified_schema_existed": False,
            "column_mapping": column_mapping,
            "gaps": gaps,
        }
    else:
        # Subsequent run — diff against unified schema
        logger.info("Unified schema found — diffing against source")

        # Filter out computed and enrichment columns for the prompt
        mappable_cols = {
            name: spec for name, spec in unified["columns"].items()
            if not spec.get("computed") and not spec.get("enrichment")
        }
        unified_for_prompt = {"columns": mappable_cols}

        result = call_llm_json(
            model=model,
            messages=[{
                "role": "user",
                "content": SCHEMA_ANALYSIS_PROMPT.format(
                    source_schema=json.dumps(source_schema, indent=2),
                    unified_schema=json.dumps(unified_for_prompt, indent=2),
                ),
            }],
        )

        column_mapping = result.get("column_mapping", {})
        gaps = result.get("gaps", [])

        logger.info(f"Schema analysis: {len(column_mapping)} mappings, {len(gaps)} gaps")

        return {
            "unified_schema": unified,
            "unified_schema_existed": True,
            "column_mapping": column_mapping,
            "gaps": gaps,
        }


def check_registry_node(state: PipelineState) -> dict:
    """
    Check function registry for existing transforms that cover the gaps.

    Splits gaps into registry_hits (reusable) and registry_misses (need Agent 2).
    """
    gaps = state.get("gaps", [])
    registry = FunctionRegistry(REGISTRY_DIR)

    hits = {}
    misses = []

    for gap in gaps:
        source_type = gap.get("source_type", "string") or "string"
        target_type = gap.get("target_type", "string")
        target_col = gap.get("target_column", "")

        # Look up by type signature
        match = registry.lookup(
            source_type=source_type,
            target_type=target_type,
            tags=[target_col],
        )

        if match:
            logger.info(f"Registry hit for gap '{target_col}': {match['key']}")
            hits[match["key"]] = str(REGISTRY_DIR / match["file"])
        else:
            logger.info(f"Registry miss for gap '{target_col}'")
            misses.append(gap)

    return {
        "registry_hits": hits,
        "registry_misses": misses,
        "retry_count": 0,
        "max_retries": 2,
    }
