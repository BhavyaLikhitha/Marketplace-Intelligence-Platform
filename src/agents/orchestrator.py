"""Agent 1 — Orchestrator: schema analysis, gap detection, registry check."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.agents.state import PipelineState
from src.agents.prompts import SCHEMA_ANALYSIS_PROMPT
from src.models.llm import call_llm_json, get_orchestrator_llm
from src.schema.analyzer import (
    profile_dataframe,
    load_unified_schema,
)
from src.registry.block_registry import BlockRegistry
from src.blocks.mapping_io import write_mapping_yaml, merge_hitl_decisions
from src.blocks.dynamic_mapping import DynamicMappingBlock

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_BLOCK_COLUMN_PROVIDERS: dict[str, str] = {
    "allergens": "extract_allergens",
    "primary_category": "llm_enrich",
    "dietary_tags": "llm_enrich",
    "is_organic": "llm_enrich",
}


def _detect_enrichment_columns(unified_schema: dict, source_schema: dict) -> list[str]:
    """Return names of enrichment columns in the unified schema absent from source data."""
    source_cols = set(source_schema.keys())
    return [
        name
        for name, spec in unified_schema.get("columns", {}).items()
        if spec.get("enrichment") and name not in source_cols
    ]


def load_source_node(state: PipelineState) -> dict:
    """Load CSV and compute schema profile."""
    if state.get("source_df") is not None:
        return {}
    source_path = state["source_path"]
    logger.info(f"Loading source: {source_path}")

    df = pd.read_csv(source_path)
    schema = profile_dataframe(df)

    return {
        "source_df": df,
        "source_schema": schema,
    }


def _parse_llm_response(result: dict) -> tuple[dict, list, list]:
    """Parse LLM schema analysis response, handling both old and new formats.

    Returns:
        (column_mapping, derivable_gaps, missing_columns)
    """
    column_mapping = result.get("column_mapping", {})

    # New format: derivable_gaps + missing_columns
    if "derivable_gaps" in result or "missing_columns" in result:
        derivable_gaps = result.get("derivable_gaps", [])
        missing_columns = result.get("missing_columns", [])
        return column_mapping, derivable_gaps, missing_columns

    # Fallback: old format with flat "gaps" list.
    # Heuristically split: source_column is None → MISSING, else derivable.
    gaps = result.get("gaps", [])
    derivable_gaps = []
    missing_columns = []
    for gap in gaps:
        if gap.get("source_column") is None:
            missing_columns.append({
                "target_column": gap.get("target_column", ""),
                "target_type": gap.get("target_type", "string"),
                "reason": "No source column available (auto-classified from legacy format)",
            })
        else:
            # Re-classify action based on gap characteristics
            action = gap.get("action", "GAP")
            if action == "GAP":
                src_type = gap.get("source_type", "")
                tgt_type = gap.get("target_type", "")
                action = "TYPE_CAST" if src_type != tgt_type else "FORMAT_TRANSFORM"
            gap = dict(gap, action=action)
            derivable_gaps.append(gap)

    return column_mapping, derivable_gaps, missing_columns


def analyze_schema_node(state: PipelineState) -> dict:
    """
    Agent 1 LLM call: analyze source schema against the gold-standard unified schema.

    Classifies each unified column into one of four categories:
    - MAP: direct rename (column_mapping)
    - TYPE_CAST / FORMAT_TRANSFORM: derivable from source (derivable_gaps)
    - DERIVE: computable from source columns (derivable_gaps)
    - MISSING: no source data, no derivation path (missing_columns)

    Raises FileNotFoundError if config/unified_schema.json is absent.
    """
    if state.get("unified_schema") is not None:
        return {}
    source_schema = state["source_schema"]
    domain = state.get("domain", "nutrition")
    model = get_orchestrator_llm()

    unified = load_unified_schema()

    if unified is None:
        raise FileNotFoundError(
            "config/unified_schema.json not found. "
            "The unified schema is the gold-standard target format and must be defined before running the pipeline."
        )

    logger.info("Unified schema found — diffing against source")

    mappable_cols = {
        name: spec
        for name, spec in unified["columns"].items()
        if not spec.get("computed") and not spec.get("enrichment")
    }
    unified_for_prompt = {"columns": mappable_cols}

    result = call_llm_json(
        model=model,
        messages=[
            {
                "role": "user",
                "content": SCHEMA_ANALYSIS_PROMPT.format(
                    source_schema=json.dumps(source_schema, indent=2),
                    unified_schema=json.dumps(unified_for_prompt, indent=2),
                ),
            }
        ],
    )

    column_mapping, derivable_gaps, missing_columns = _parse_llm_response(result)

    logger.info(
        f"Schema analysis: {len(column_mapping)} mappings, "
        f"{len(derivable_gaps)} derivable gaps, "
        f"{len(missing_columns)} missing columns"
    )

    # Synthesize backward-compat gaps list (union of derivable + missing)
    gaps = list(derivable_gaps)
    for mc in missing_columns:
        gaps.append({
            "target_column": mc["target_column"],
            "target_type": mc["target_type"],
            "source_column": None,
            "source_type": None,
            "action": "MISSING",
            "sample_values": [],
        })

    required_mappable = {
        name
        for name, spec in unified["columns"].items()
        if spec.get("required")
        and not spec.get("computed")
        and not spec.get("enrichment")
    }
    covered = (
        set(column_mapping.values())
        | {g["target_column"] for g in derivable_gaps}
        | {mc["target_column"] for mc in missing_columns}
    )
    mapping_warnings = [
        f"Required unified column '{col}' not covered by mapping or gaps"
        for col in sorted(required_mappable - covered)
    ]
    for w in mapping_warnings:
        logger.warning(w)

    enrichment_to_generate = _detect_enrichment_columns(unified, source_schema)
    if enrichment_to_generate:
        logger.info(
            f"Enrichment columns absent from source (will be generated by blocks): "
            f"{enrichment_to_generate}"
        )

    if missing_columns:
        logger.warning(
            f"Missing columns (no source data): "
            f"{[mc['target_column'] for mc in missing_columns]}"
        )

    return {
        "unified_schema": unified,
        "unified_schema_existed": True,
        "column_mapping": column_mapping,
        "gaps": gaps,
        "derivable_gaps": derivable_gaps,
        "missing_columns": missing_columns,
        "enrichment_columns_to_generate": enrichment_to_generate,
        "mapping_warnings": mapping_warnings,
    }


def check_registry_node(state: PipelineState) -> dict:
    """
    Check BlockRegistry for existing blocks, then build a YAML mapping file
    for simple operations (missing columns, type casts, format transforms).

    Only genuinely complex DERIVE gaps without existing blocks remain as
    registry_misses for Agent 2 code generation.

    Three phases:
    A. Missing columns → write as set_null operations to YAML
    B. Derivable gaps → check registry; simple TYPE_CAST/FORMAT_TRANSFORM
       without hits go to YAML; DERIVE gaps without hits go to registry_misses
    C. If YAML operations exist, create and register DynamicMappingBlock
    """
    if "block_registry_hits" in state:
        return {}

    block_reg = BlockRegistry.instance()
    domain = state.get("domain", "nutrition")
    dataset_name = Path(state.get("source_path", "unknown")).stem
    column_mapping = state.get("column_mapping", {})
    missing_columns = state.get("missing_columns", [])
    derivable_gaps = state.get("derivable_gaps", [])
    decisions = state.get("missing_column_decisions", {})

    block_hits: dict[str, str] = {}  # target_col -> block_name
    misses = []  # only DERIVE gaps without registry hits
    yaml_operations: list[dict] = []

    # ── Phase A: Missing columns → YAML set_null ────────────────────
    for mc in missing_columns:
        target_col = mc["target_column"]
        target_type = mc["target_type"]

        # Check if an enrichment block handles this
        provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
        if provider and provider in block_reg.blocks:
            logger.info(f"Block provider for missing column '{target_col}': {provider}")
            block_hits[target_col] = provider
            continue

        yaml_operations.append({
            "target": target_col,
            "type": target_type,
            "action": "set_null",
            "status": "missing",
            "reason": mc.get("reason", "No source data available"),
        })
        logger.info(f"Missing column '{target_col}' → YAML set_null")

    # ── Phase B: Derivable gaps → registry check or YAML ────────────
    generated_block_prefixes = (
        "TYPE_CONVERSION_",
        "COLUMN_RENAME_",
        "COLUMN_DROP_",
        "COLUMN_CREATE_",
        "FORMAT_TRANSFORM_",
        "DYNAMIC_MAPPING_",
        "DERIVE_",
    )

    for gap in derivable_gaps:
        source_col = gap.get("source_column")
        target_col = gap.get("target_column", "")
        target_type = gap.get("target_type", "string")
        source_type = gap.get("source_type") or "string"
        action = gap.get("action", "TYPE_CAST")

        # Check enrichment providers first
        provider = _BLOCK_COLUMN_PROVIDERS.get(target_col)
        if provider and provider in block_reg.blocks:
            logger.info(f"Block registry hit for gap '{target_col}': {provider}")
            block_hits[target_col] = provider
            continue

        # Check for existing generated blocks
        found_existing = False
        for block_name in block_reg.blocks.keys():
            if block_name.startswith(generated_block_prefixes):
                if target_col in block_name or block_name.endswith(f"_{target_col}"):
                    logger.info(f"Generated block found for gap '{target_col}': {block_name}")
                    block_hits[target_col] = block_name
                    found_existing = True
                    break

        if found_existing:
            continue

        # No existing block — route based on gap action
        if action == "DERIVE":
            # Complex derivation → Agent 2 must generate code
            logger.info(f"DERIVE gap '{target_col}' → registry miss (Agent 2)")
            misses.append(gap)
        elif action in ("TYPE_CAST", "FORMAT_TRANSFORM"):
            # Simple operation → handle via YAML
            # Resolve source column through column_mapping (runner renames first)
            effective_source = column_mapping.get(source_col, source_col) if source_col else None
            op = {
                "target": target_col,
                "type": target_type,
                "action": "type_cast" if action == "TYPE_CAST" else "format_transform",
                "source": effective_source,
                "source_type": source_type,
            }
            if action == "FORMAT_TRANSFORM":
                op["transform"] = "to_string"  # default; could be enhanced
            yaml_operations.append(op)
            logger.info(f"{action} gap '{target_col}' → YAML")
        else:
            # Unknown action type → fall back to Agent 2
            misses.append(gap)

    # ── Phase C: Apply HITL decisions, patch schema, and write YAML ──
    # For "exclude" decisions: mark the column as not required in the
    # unified schema so it won't trigger quarantine downstream.
    import copy

    unified_schema = copy.deepcopy(state.get("unified_schema", {}))
    excluded_columns = []
    for col_name, decision in decisions.items():
        if decision.get("action") == "exclude":
            col_spec = unified_schema.get("columns", {}).get(col_name)
            if col_spec:
                col_spec["required"] = False
                excluded_columns.append(col_name)
                logger.info(f"Excluded '{col_name}' from required schema (HITL decision)")

    if yaml_operations:
        yaml_operations = merge_hitl_decisions(yaml_operations, decisions)
        yaml_path = write_mapping_yaml(domain, dataset_name, yaml_operations)

        # Register the DynamicMappingBlock
        block = DynamicMappingBlock(domain=domain, yaml_path=str(yaml_path))
        block_reg.register_block(block)
        logger.info(f"Registered DynamicMappingBlock: {block.name}")

        mapping_yaml_path = str(yaml_path)
    else:
        mapping_yaml_path = None

    result = {
        "block_registry_hits": block_hits,
        "registry_misses": misses,
        "mapping_yaml_path": mapping_yaml_path,
        "retry_count": 0,
        "max_retries": 2,
    }

    # Only update unified_schema in state if we actually excluded columns
    if excluded_columns:
        result["unified_schema"] = unified_schema

    return result
