"""Streamlit UI - ETL Pipeline Wizard.

Rebuilt from scratch to align with the 3-agent LangGraph pipeline architecture.
Maps 5 wizard steps to 7 pipeline nodes, displays Agent 1/2/3 activity distinctly,
and implements all HITL gates correctly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import structlog
from streamlit import session_state as ss

from src.agents.graph import NODE_MAP, run_step
from src.agents.state import PipelineState
from src.schema.sampling import SamplingStrategy
from src.agents.confidence import get_confidence_display
from src.ui.components import (
    render_step_bar,
    render_source_profile,
    render_schema_delta,
    render_dq_cards,
    render_enrichment_breakdown,
    render_quarantine_table,
    render_agent_header,
    render_sampling_stats,
    render_confidence_badge,
    render_extraction_only_flag,
    render_hitl_gate,
    render_block_waterfall,
    render_registry_results,
)
from src.ui.styles import GLOBAL_CSS

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

WIZARD_STEPS = [
    "1. Select Source",
    "2. Schema Analysis",
    "3. Pipeline Planning",
    "4. Execution",
    "5. Results",
]

AGENT_LABELS = {
    1: ("Orchestrator", "Schema Analysis"),
    2: ("Critic", "Schema Validation"),
    3: ("Sequence Planner", "Execution Order"),
}

SAFETY_COLUMNS = {"allergens", "is_organic", "dietary_tags"}


def init_session():
    """Initialize Streamlit session state."""
    if "step" not in ss:
        ss.step = 0
    if "max_completed" not in ss:
        ss.max_completed = -1
    if "state" not in ss:
        ss.state: PipelineState = {}
    if "runs" not in ss:
        ss.runs = []
    if "pipeline_logs" not in ss:
        ss.pipeline_logs = []


def log_event(level: str, message: str, **kwargs):
    """Add a log event to session state and output to structlog."""
    timestamp = datetime.now().isoformat()
    log_entry = {"timestamp": timestamp, "level": level, "message": message, **kwargs}
    ss.pipeline_logs.append(log_entry)
    getattr(logger, level)(message, **kwargs)


def can_navigate(step: int) -> bool:
    """Check if user can navigate to a step."""
    return step <= ss.max_completed


def navigate_to_step(step: int):
    """Navigate to a specific step if allowed."""
    if step >= 0 and step < len(WIZARD_STEPS):
        if step <= ss.max_completed:
            ss.step = step
            st.rerun()


def step_select_source() -> bool:
    """Step 1: Select source CSV file and domain."""
    st.header("Select Data Source")
    log_event("info", "Step 1: Select Source - entering")

    st.markdown("Choose a CSV file from the `data/` directory and specify the domain.")

    csv_files = []
    if DATA_DIR.exists():
        csv_files = [f.name for f in DATA_DIR.glob("*.csv")]

    if not csv_files:
        st.warning("No CSV files found in `data/` directory.")
        st.info("Add CSV files to the `data/` folder to get started.")
        return False

    selected_file = st.selectbox(
        "Source File",
        csv_files,
        index=0 if not ss.state.get("source_path") else None,
    )

    domain = st.selectbox(
        "Domain",
        ["nutrition", "safety", "pricing"],
        index=0 if ss.state.get("domain") != "nutrition" else None,
    )

    enable_enrichment = st.toggle("Enable Enrichment", value=True)

    if st.button("Load Source", type="primary"):
        source_path = str(DATA_DIR / selected_file)
        try:
            log_event("info", f"Loading CSV file: {selected_file}", file=selected_file)
            df = pd.read_csv(source_path)
            log_event(
                "info",
                f"Loaded {len(df)} rows, {len(df.columns)} columns",
                rows=len(df),
                columns=len(df.columns),
            )
            ss.state["source_path"] = source_path
            ss.state["source_df"] = df
            ss.state["domain"] = domain
            ss.state["enable_enrichment"] = enable_enrichment

            log_event("info", "Running load_source node")
            result = run_step("load_source", ss.state)
            ss.state.update(result)

            log_event(
                "info",
                "load_source node completed",
                source_schema_keys=len(ss.state.get("source_schema", {})),
            )
            ss.max_completed = max(ss.max_completed, 0)
            ss.step = 1
            st.rerun()
        except Exception as e:
            log_event("error", f"Failed to load source: {e}", error=str(e))
            st.error(f"Failed to load source: {e}")
            return False

    return True


def step_schema_analysis() -> bool:
    """Step 2: Schema analysis with Agent 1 and Agent 2."""
    st.header("Schema Analysis")
    log_event("info", "Step 2: Schema Analysis - entering")

    st.markdown(
        render_agent_header(
            1,
            AGENT_LABELS[1][0],
            "Analyzing source schema and identifying gaps",
        ),
        unsafe_allow_html=True,
    )

    source_schema = ss.state.get("source_schema", {})
    if source_schema:
        log_event(
            "info",
            f"Source schema analyzed: {len(source_schema)} columns",
            columns=len(source_schema),
        )
        st.subheader("Source Schema Profile")
        st.markdown(render_source_profile(source_schema), unsafe_allow_html=True)

    sampling_strategy = ss.state.get("sampling_strategy")
    if sampling_strategy:
        st.subheader("Sampling Statistics")
        st.markdown(
            render_sampling_stats(
                {
                    "method": sampling_strategy.get("method", "unknown"),
                    "sample_size": sampling_strategy.get("sample_size", 0),
                    "fallback_triggered": sampling_strategy.get(
                        "fallback_triggered", False
                    ),
                    "fallback_reason": sampling_strategy.get("fallback_reason", ""),
                }
            ),
            unsafe_allow_html=True,
        )

    unified_schema = ss.state.get("unified_schema")
    column_mapping = ss.state.get("column_mapping", {})
    derivable_gaps = ss.state.get("derivable_gaps", [])
    missing_columns = ss.state.get("missing_columns", [])
    enrichment_columns = ss.state.get("enrichment_columns_to_generate", [])
    enrich_alias_ops = ss.state.get("enrich_alias_ops", [])

    if column_mapping or derivable_gaps or missing_columns or enrichment_columns:
        st.subheader("Schema Delta")
        st.markdown(
            render_schema_delta(
                source_profile=source_schema,
                column_mapping=column_mapping,
                gaps=ss.state.get("gaps", []),
                unified_schema=unified_schema,
                missing_columns=missing_columns,
                derivable_gaps=derivable_gaps,
                enrichment_columns=enrichment_columns,
                enrich_alias_ops=enrich_alias_ops,
            ),
            unsafe_allow_html=True,
        )

    if "operations" in ss.state:
        st.subheader("Proposed Operations")
        ops = ss.state.get("operations", [])
        if ops:
            for op in ops:
                target = op.get("target", "?")
                is_safety = target in SAFETY_COLUMNS
                safety_badge = render_extraction_only_flag() if is_safety else ""
                action = op.get("action", "?")
                st.markdown(
                    f"- **{target}**: {action} {safety_badge}",
                    unsafe_allow_html=bool(safety_badge),
                )

    st.markdown(
        render_agent_header(
            2,
            AGENT_LABELS[2][0],
            "Reviewing schema mapping and validating operations",
        ),
        unsafe_allow_html=True,
    )

    if "revised_operations" in ss.state:
        st.subheader("Critique Notes")
        for note in ss.state.get("critique_notes", []):
            st.info(note.get("description", ""))

    st.markdown(
        render_hitl_gate(
            1,
            "Schema Mapping Approval",
            ["Approve & Continue", "Exclude Column", "Abort"],
        ),
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Approve & Continue", type="primary"):
            ss.state["hitl_decision_gate1"] = "approve"
            result = run_step("check_registry", ss.state)
            ss.state.update(result)
            ss.max_completed = max(ss.max_completed, 1)
            ss.step = 2
            st.rerun()
    with col2:
        if st.button("Exclude Column"):
            ss.state["hitl_decision_gate1"] = "exclude"
            ss.step = 2
            st.rerun()
    with col3:
        if st.button("Abort", type="secondary"):
            ss.state["hitl_decision_gate1"] = "abort"
            st.error("Pipeline aborted by user.")
            return False

    return True


def step_critique_and_plan() -> bool:
    """Step 3: Pipeline planning with Agent 3."""
    st.header("Pipeline Planning")
    log_event("info", "Step 3: Pipeline Planning - entering")

    st.markdown(
        render_agent_header(
            3,
            AGENT_LABELS[3][0],
            "Determining optimal block execution order",
        ),
        unsafe_allow_html=True,
    )

    block_registry_hits = ss.state.get("block_registry_hits", {})
    registry_misses = ss.state.get("registry_misses", [])

    if block_registry_hits or registry_misses:
        log_event(
            "info",
            f"Registry check: {len(block_registry_hits)} hits, {len(registry_misses)} misses",
            hits=len(block_registry_hits),
            misses=len(registry_misses),
        )
        st.subheader("Block Registry Results")
        st.markdown(
            render_registry_results(block_registry_hits, registry_misses),
            unsafe_allow_html=True,
        )

    block_sequence = ss.state.get("block_sequence", [])
    sequence_reasoning = ss.state.get("sequence_reasoning", "")

    if block_sequence:
        st.subheader("Execution Sequence")
        reasoning_expander = st.expander("Agent 3 Reasoning", expanded=False)
        with reasoning_expander:
            st.markdown(sequence_reasoning or "No reasoning provided.")

        st.markdown("**Block Order:**")
        for i, block in enumerate(block_sequence, 1):
            st.markdown(f"{i}. `{block}`")

    if st.button("Execute Pipeline", type="primary"):
        log_event("info", "Starting pipeline execution")
        log_event(
            "info", f"Block sequence: {len(ss.state.get('block_sequence', []))} blocks"
        )
        result = run_step("run_pipeline", ss.state)
        ss.state.update(result)
        log_event("info", "Pipeline execution completed")

        result2 = run_step("save_output", ss.state)
        ss.state.update(result2)
        log_event("info", "Output saved", output=ss.state.get("output_path", ""))

        output_path = ss.state.get("output_path", "")
        if output_path:
            st.success(f"Pipeline completed. Output saved to: {output_path}")

            run_record = {
                "run_num": len(ss.runs) + 1,
                "source": Path(ss.state.get("source_path", "")).name,
                "domain": ss.state.get("domain", "nutrition"),
                "rows": len(ss.state.get("working_df", pd.DataFrame())),
                "dq_pre": ss.state.get("dq_score_pre", 0),
                "dq_post": ss.state.get("dq_score_post", 0),
                "dq_delta": ss.state.get("dq_score_post", 0)
                - ss.state.get("dq_score_pre", 0),
                "registry_hits": len(block_registry_hits),
                "functions_generated": len(registry_misses),
                "schema_existed": ss.state.get("unified_schema_existed", False),
            }
            ss.runs.append(run_record)

        ss.max_completed = max(ss.max_completed, 2)
        ss.step = 3
        st.rerun()

    return True


def step_execution() -> bool:
    """Step 4: Execution progress (already runs in step 3)."""
    st.header("Execution")
    log_event("info", "Step 4: Execution - entering")

    st.success("Pipeline execution completed!")

    audit_log = ss.state.get("audit_log", [])
    if audit_log:
        log_event("info", f"Block waterfall: {len(audit_log)} blocks executed")
        st.subheader("Block Execution Waterfall")
        st.markdown(
            render_block_waterfall(audit_log),
            unsafe_allow_html=True,
        )

    st.markdown(
        render_hitl_gate(
            3,
            "Quarantine Acceptance",
            ["Accept Quarantine", "Override: Include All"],
        ),
        unsafe_allow_html=True,
    )

    quarantined_df = ss.state.get("quarantined_df")
    quarantine_reasons = ss.state.get("quarantine_reasons", [])

    if quarantine_reasons:
        log_event(
            "warning",
            f"{len(quarantine_reasons)} rows quarantined",
            quarantined=len(quarantine_reasons),
        )
        st.warning(f"{len(quarantine_reasons)} rows were quarantined.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Accept Quarantine", type="primary"):
            ss.state["hitl_decision_gate3"] = "accept"
            ss.max_completed = max(ss.max_completed, 3)
            ss.step = 4
            st.rerun()
    with col2:
        if st.button("Override: Include All"):
            ss.state["hitl_decision_gate3"] = "override"
            df = ss.state.get("working_df", pd.DataFrame())
            quarantined = ss.state.get("quarantined_df")
            if quarantined is not None and len(quarantined) > 0:
                ss.state["working_df"] = pd.concat([df, quarantined], ignore_index=True)
            ss.max_completed = max(ss.max_completed, 3)
            ss.step = 4
            st.rerun()

    return True


def step_results() -> bool:
    """Step 5: Display final results."""
    st.header("Results")
    log_event("info", "Step 5: Results - entering")

    dq_pre = ss.state.get("dq_score_pre", 0)
    dq_post = ss.state.get("dq_score_post", 0)

    log_event(
        "info",
        f"DQ Scores - Pre: {dq_pre}%, Post: {dq_post}%",
        dq_pre=dq_pre,
        dq_post=dq_post,
    )
    st.subheader("Data Quality Scores")
    st.markdown(
        render_dq_cards(dq_pre, dq_post),
        unsafe_allow_html=True,
    )

    enrichment_stats = ss.state.get("enrichment_stats", {})
    if enrichment_stats:
        log_event("info", f"Enrichment stats: {enrichment_stats}")
        st.subheader("Enrichment Statistics")
        st.markdown(
            render_enrichment_breakdown(enrichment_stats),
            unsafe_allow_html=True,
        )

    quarantined_df = ss.state.get("quarantined_df")
    quarantine_reasons = ss.state.get("quarantine_reasons", [])

    if quarantine_reasons:
        st.subheader("Quarantine Table")
        st.markdown(
            render_quarantine_table(
                quarantine_reasons,
                quarantined_df if quarantined_df is not None else None,
            ),
            unsafe_allow_html=True,
        )

    output_path = ss.state.get("output_path", "")
    if output_path:
        st.subheader("Output")
        st.success(f"Output saved to: `{output_path}`")

        working_df = ss.state.get("working_df")
        if working_df is not None and len(working_df) > 0:
            st.dataframe(working_df.head(50))

    if ss.runs:
        st.subheader("Run History")
        st.markdown(render_run_history(ss.runs), unsafe_allow_html=True)

    if st.button("Start New Pipeline"):
        log_event("info", "Starting new pipeline - resetting state")
        ss.state.clear()
        ss.step = 0
        ss.max_completed = -1
        ss.pipeline_logs = []
        st.rerun()

    return True


def render_sidebar():
    """Render sidebar navigation."""
    st.sidebar.header("Navigation")

    step_names = [
        "1. Select Source",
        "2. Schema Analysis",
        "3. Pipeline Planning",
        "4. Execution",
        "5. Results",
    ]

    for i, name in enumerate(step_names):
        disabled = i > ss.max_completed + 1
        if st.sidebar.button(name, disabled=disabled):
            if i <= ss.max_completed + 1:
                ss.step = i
                st.rerun()

    st.sidebar.divider()

    st.sidebar.markdown("**Pipeline Logs**")
    logs = ss.pipeline_logs[-20:] if len(ss.pipeline_logs) > 20 else ss.pipeline_logs
    if logs:
        for log in logs:
            level = log.get("level", "info")
            msg = log.get("message", "")
            ts = log.get("timestamp", "")[11:19]
            color = {"info": "#0969da", "warning": "#9a6700", "error": "#cf222e"}.get(
                level, "#57606a"
            )
            st.sidebar.markdown(
                f'<span style="color:{color}">[{ts}] {msg}</span>',
                unsafe_allow_html=True,
            )
    else:
        st.sidebar.markdown("*No logs yet*")


def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="ETL Pipeline Wizard",
        page_icon=":rocket:",
        layout="wide",
    )

    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    init_session()

    st.title("ETL Pipeline Wizard")
    st.markdown(
        "**7-node pipeline** with **3 agents** (Orchestrator → Critic → Sequence Planner)"
    )

    render_sidebar()

    st.markdown(
        render_step_bar(ss.step, WIZARD_STEPS, ss.max_completed),
        unsafe_allow_html=True,
    )

    step_handlers = [
        step_select_source,
        step_schema_analysis,
        step_critique_and_plan,
        step_execution,
        step_results,
    ]

    if 0 <= ss.step < len(step_handlers):
        step_handlers[ss.step]()


if __name__ == "__main__":
    main()
