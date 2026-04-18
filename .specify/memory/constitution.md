# Schema-Driven ETL Pipeline Constitution

<!--
Sync Impact Report:
- Version: 1.2.0 → 1.2.1 (PATCH)
- Changes: Renamed "Agent 1.5" to "Agent 2" throughout
- Principles modified: I (Agent 1.5 → Agent 2), II (Agent 1.5 → Agent 2)
- Pipeline flow modified: Node 3 renamed "Agent 1.5" → "Agent 2"
- Templates: No constitution-specific references to update
-->

## Core Principles

### I. Schema-First Gap Analysis
Every incoming data source MUST be analyzed against the unified output schema defined in `config/unified_schema.json`. The system MUST detect and classify all schema gaps using the 8-primitive taxonomy: RENAME, CAST, FORMAT, DELETE, ADD, SPLIT, UNIFY, DERIVE. Gap classification MUST be performed by the DeepSeek LLM (Agent 1) with validation by a reasoning model (Agent 2).

### II. Two-Agent Architecture with Critic Validation
The pipeline uses a two-agent-plus-critic architecture (not three agents):
- **Agent 1 (Orchestrator)**: Schema analysis and gap detection via LLM
- **Agent 2 (Critic)**: Validates and corrects Agent 1's operations using a reasoning model
- **Agent 3 (Sequence Planner)**: Determines optimal block execution order via LLM

There is NO Agent 2 for code generation — all gaps are resolved declaratively via YAML, not LLM-generated Python.

### III. Declarative YAML-Driven Transformations
All schema transformations MUST be expressed declaratively in YAML mapping files. The DynamicMappingBlock executes these operations deterministically without LLM code generation. This eliminates the need for sandbox execution and provides full auditability. YAML operations cover: set_null, set_default, type_cast, rename, drop_column, format_transform, value_map, regex operations, split operations (json_array_extract_multi, split_column, xml_extract), unify operations (coalesce, concat_columns, string_template), and derive operations (extract_json_field, conditional_map, expression, contains_flag).

### IV. Human-in-the-Loop (HITL) Approval
The Streamlit UI MUST expose approval gates at critical decision points:
- **Gate 1**: Schema mapping approval — user reviews column mapping, derivable gaps, missing columns, and can exclude columns from required schema
- **Gate 2**: No explicit code review gate (no Agent 2 exists)
- **Gate 3**: Quarantine acceptance — user can accept quarantine or override to include all rows

All HITL decisions MUST be merged into YAML before pipeline execution.

### V. Cascading Enrichment Strategy
Enrichment of missing columns proceeds through three strategies in order of increasing cost:
1. **S1 (Deterministic)**: Regex/keyword extraction — handles primary_category, allergens, dietary_tags, is_organic
2. **S2 (KNN Corpus)**: FAISS-based product-to-product similarity search — primary_category only
3. **S3 (RAG-LLM)**: Retrieval-augmented LLM with corpus examples — primary_category only

**Safety constraint**: allergens, is_organic, and dietary_tags are extraction-only fields. S2 and S3 MUST NOT modify them — they are populated only by S1 extraction from source text.

### VI. Self-Extending Pipeline Memory
Generated YAML mapping files MUST be persisted to `src/blocks/generated/<domain>/`. On subsequent runs with the same source, the BlockRegistry MUST auto-discover existing mappings and skip LLM analysis — achieving zero LLM cost for replayed sources.

### VII. Data Quality Scoring
Data quality scores MUST be computed at two points: before enrichment (dq_score_pre) and after (dq_score_post). The formula is: DQ Score = (Completeness × 0.4) + (Freshness × 0.35) + (Ingredient Richness × 0.25). Rows failing required column validation after enrichment MUST be quarantined with user override capability.

## Technology Stack

- **UI Framework**: Streamlit — 5-step wizard interface
- **Orchestration**: LangGraph — StateGraph with 7 nodes
- **LLM Provider**: DeepSeek (deepseek-chat, deepseek-reasoner) via LiteLLM
- **Data Processing**: pandas — DataFrame manipulation
- **Vector Store**: FAISS — KNN-based product similarity for S2 enrichment
- **Fuzzy Matching**: rapidfuzz — deduplication clustering
- **Block Discovery**: importlib — dynamic block loading from src/blocks/generated/

## Development Workflow

### Pipeline Execution Flow (7 nodes)
1. **load_source**: Load CSV, profile schema (dtype, null_rate, unique_count, sample_values, detected_structure)
2. **analyze_schema**: Agent 1 LLM call for gap detection using 8-primitive taxonomy
3. **critique_schema**: Agent 2 validates/corrects Agent 1's operations
4. **check_registry**: Build YAML mapping, merge HITL decisions, register DynamicMappingBlock
5. **plan_sequence**: Agent 3 determines block execution order
6. **run_pipeline**: Sequential block execution with audit logging
7. **save_output**: Write final DataFrame to output/

### Block Execution Order
The default sequence is: dq_score_pre → __generated__ (DynamicMappingBlock) → strip_whitespace → lowercase_brand → remove_noise_words → strip_punctuation → extract_quantity_column → dedup_stage (fuzzy_deduplicate, column_wise_merge, golden_record_select) → enrich_stage (extract_allergens, llm_enrich) → dq_score_post

### Quality Gates
- All schema operations converted to YAML (no runtime code generation)
- DQ scores computed pre/post with delta tracking
- Quarantine logic applied to rows with null required columns
- Enrichment stats logged for strategy breakdown (S1/S2/S3 counts)

## Governance

This constitution supersedes all other practices. Amendments require:
- Documentation of the principle change or addition
- Clear rationale for the modification
- Update to constitution version following semantic versioning:
  - MAJOR: Backward incompatible governance/principle removals or redefinitions
  - MINOR: New principle/section added or materially expanded guidance
  - PATCH: Clarifications, wording, typo fixes

**Version**: 1.2.1 | **Ratified**: 2026-04-17 | **Last Amended**: 2026-04-17