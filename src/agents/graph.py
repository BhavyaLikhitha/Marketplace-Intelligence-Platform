"""LangGraph StateGraph + step-by-step runner for the Streamlit UI."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from langgraph.graph import StateGraph, END

from src.agents.state import PipelineState
from src.agents.orchestrator import (
    load_source_node,
    analyze_schema_node,
    check_registry_node,
)
from src.agents.code_generator import (
    generate_code_node,
    validate_code_node,
    register_functions_node,
)
from src.registry.block_registry import BlockRegistry
from src.registry.function_registry import FunctionRegistry
from src.pipeline.runner import PipelineRunner

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REGISTRY_DIR = PROJECT_ROOT / "function_registry"
OUTPUT_DIR = PROJECT_ROOT / "output"


# ── Pipeline execution nodes ─────────────────────────────────────────


def run_pipeline_node(state: PipelineState) -> dict:
    """Execute the block sequence on the working DataFrame."""
    block_registry = BlockRegistry()
    function_registry = FunctionRegistry(REGISTRY_DIR)
    runner = PipelineRunner(block_registry, function_registry)

    domain = state.get("domain", "nutrition")
    block_sequence = block_registry.get_default_sequence(domain)

    # Collect all functions (registry hits + newly generated)
    all_functions = []

    # Registry hits
    for key, file_path in (state.get("registry_hits") or {}).items():
        entries = function_registry.list_all()
        entry = next((e for e in entries if e["key"] == key), None)
        if entry:
            all_functions.append(
                {
                    "function_name": entry["function_name"],
                    "file_path": file_path,
                    "registry_key": key,
                }
            )

    # Newly generated functions
    for func in state.get("generated_functions") or []:
        if func.get("validation_passed") and func.get("file_path"):
            all_functions.append(func)

    config = {
        "dq_weights": (state.get("unified_schema") or {}).get("dq_weights"),
        "domain": domain,
    }

    df = state["source_df"].copy()
    column_mapping = state.get("column_mapping", {})

    result_df, audit_log = runner.run(
        df=df,
        block_sequence=block_sequence,
        generated_functions=all_functions,
        column_mapping=column_mapping,
        config=config,
    )

    # Extract DQ scores
    dq_pre = (
        float(result_df["dq_score_pre"].mean())
        if "dq_score_pre" in result_df.columns
        else 0.0
    )
    dq_post = (
        float(result_df["dq_score_post"].mean())
        if "dq_score_post" in result_df.columns
        else 0.0
    )

    # Extract enrichment stats from the llm_enrich block instance
    enrichment_stats = {}
    try:
        enrich_block = block_registry.get("llm_enrich")
        enrichment_stats = getattr(enrich_block, "last_enrichment_stats", {})
    except Exception:
        pass

    # Post-enrichment quarantine: rows with nulls in required fields
    unified_schema = state.get("unified_schema", {})
    required_cols = [
        col
        for col, spec in unified_schema.get("columns", {}).items()
        if spec.get("required") and not spec.get("computed")
    ]

    # Only quarantine rows where existing columns have nulls (not missing columns)
    existing_required = [c for c in required_cols if c in result_df.columns]
    missing_cols = [c for c in required_cols if c not in result_df.columns]

    if missing_cols:
        logger.warning(
            f"Schema mismatch: {len(missing_cols)} required columns missing from output: {missing_cols}"
        )
        # Don't quarantine just because columns are missing - that's a schema mismatch issue, not a data issue
        # Only quarantine rows with nulls in columns that actually exist
        quarantined_mask = (
            result_df[existing_required].isna().any(axis=1)
            if existing_required
            else pd.Series(False, index=result_df.index)
        )
    else:
        quarantined_mask = (
            result_df[required_cols].isna().any(axis=1)
            if required_cols
            else pd.Series(False, index=result_df.index)
        )
    quarantined_df = result_df[quarantined_mask].copy()
    clean_df = result_df[~quarantined_mask].copy()

    quarantine_reasons = []
    for idx in quarantined_df.index:
        missing = [c for c in required_cols if pd.isna(quarantined_df.at[idx, c])]
        quarantine_reasons.append(
            {
                "row_idx": int(idx),
                "missing_fields": missing,
                "reason": f"Null in required field(s): {', '.join(missing)}",
            }
        )

    if len(quarantined_df) > 0:
        logger.info(
            f"Quarantine: {len(quarantined_df)} rows failed post-enrichment validation"
        )

    return {
        "working_df": clean_df,
        "quarantined_df": quarantined_df,
        "quarantine_reasons": quarantine_reasons,
        "block_sequence": block_sequence,
        "audit_log": audit_log,
        "enrichment_stats": enrichment_stats,
        "dq_score_pre": round(dq_pre, 2),
        "dq_score_post": round(dq_post, 2),
    }


def save_output_node(state: PipelineState) -> dict:
    """Save the final DataFrame to output/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_path = state.get("source_path", "unknown")
    source_name = Path(source_path).stem
    output_path = OUTPUT_DIR / f"{source_name}_unified.csv"

    df = state["working_df"]
    df.to_csv(output_path, index=False)
    logger.info(f"Output saved to {output_path} ({len(df)} rows)")

    return {"output_path": str(output_path)}


# ── Routing functions ────────────────────────────────────────────────


def route_after_registry_check(state: PipelineState) -> str:
    misses = state.get("registry_misses", [])
    if misses:
        logger.info(f"{len(misses)} registry misses — routing to code generator")
        return "generate_code"
    logger.info("All gaps covered by registry — skipping code generation")
    return "run_pipeline"


def route_after_validation(state: PipelineState) -> str:
    generated = state.get("generated_functions", [])
    retry_count = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 2)

    all_passed = all(f.get("validation_passed", False) for f in generated)

    if all_passed:
        return "register_functions"
    if retry_count < max_retries:
        logger.info(f"Validation failed, retrying ({retry_count}/{max_retries})")
        return "generate_code"
    logger.warning("Max retries reached — registering partial results")
    return "register_functions"


# ── Step-by-step runner (for Streamlit UI) ───────────────────────────

NODE_MAP = {
    "load_source": load_source_node,
    "analyze_schema": analyze_schema_node,
    "check_registry": check_registry_node,
    "generate_code": generate_code_node,
    "validate_code": validate_code_node,
    "register_functions": register_functions_node,
    "run_pipeline": run_pipeline_node,
    "save_output": save_output_node,
}


def run_step(step_name: str, state: dict) -> dict:
    """
    Run a single pipeline step by name. Used by the Streamlit wizard
    to execute nodes sequentially with HITL gates in between.
    """
    if step_name not in NODE_MAP:
        raise KeyError(f"Unknown step: {step_name}. Available: {list(NODE_MAP.keys())}")

    node_fn = NODE_MAP[step_name]
    updates = node_fn(state)
    state.update(updates)
    return state


# ── Full graph builder (for CLI / non-interactive use) ───────────────


def build_graph() -> StateGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("load_source", load_source_node)
    graph.add_node("analyze_schema", analyze_schema_node)
    graph.add_node("check_registry", check_registry_node)
    graph.add_node("generate_code", generate_code_node)
    graph.add_node("validate_code", validate_code_node)
    graph.add_node("register_functions", register_functions_node)
    graph.add_node("run_pipeline", run_pipeline_node)
    graph.add_node("save_output", save_output_node)

    graph.add_edge("load_source", "analyze_schema")
    graph.add_edge("analyze_schema", "check_registry")

    graph.add_conditional_edges(
        "check_registry",
        route_after_registry_check,
        {"generate_code": "generate_code", "run_pipeline": "run_pipeline"},
    )

    graph.add_edge("generate_code", "validate_code")

    graph.add_conditional_edges(
        "validate_code",
        route_after_validation,
        {"register_functions": "register_functions", "generate_code": "generate_code"},
    )

    graph.add_edge("register_functions", "run_pipeline")
    graph.add_edge("run_pipeline", "save_output")
    graph.add_edge("save_output", END)

    graph.set_entry_point("load_source")
    return graph.compile()
