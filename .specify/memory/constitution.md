<!--
Sync Impact Report:
- Version: 1.2.1 -> 1.3.0
- Modified principles:
  - I. Schema-First Gap Analysis -> I. Schema-First Gap Analysis
  - II. Two-Agent Architecture with Critic Validation -> II. Three-Agent Pipeline with Critic Review
  - III. Declarative YAML-Driven Transformations -> III. Declarative YAML Execution Only
  - IV. Human-in-the-Loop (HITL) Approval -> IV. Human Approval Gates
  - V. Cascading Enrichment Strategy -> V. Cascading Enrichment with Safety Boundaries
  - VI. Self-Extending Pipeline Memory -> VI. Self-Extending Mapping Memory
  - VII. Data Quality Scoring -> VII. Data Quality and Quarantine Enforcement
- Added sections: None
- Removed sections: None
- Templates requiring updates:
  - ✅ updated `.specify/templates/plan-template.md`
  - ✅ updated `.specify/templates/spec-template.md`
  - ✅ updated `.specify/templates/tasks-template.md`
  - ✅ updated `README.md`
  - ✅ no command templates present under `.specify/templates/commands/`
- Follow-up TODOs: None
-->
# Schema-Driven ETL Pipeline Constitution

## Core Principles

### I. Schema-First Gap Analysis
Every ingestion flow MUST analyze the incoming dataset against
`config/unified_schema.json` before transformation planning begins. Schema gaps
MUST be classified with the 8-primitive taxonomy: `RENAME`, `CAST`, `FORMAT`,
`DELETE`, `ADD`, `SPLIT`, `UNIFY`, and `DERIVE`. Agent 1 MUST produce the
initial operations list, and Agent 2 MUST review that list before YAML mapping
registration proceeds.

Rationale: the unified schema is the contract for all downstream blocks, data
quality scoring, and output validation.

### II. Three-Agent Pipeline with Critic Review
The pipeline architecture MUST remain a three-agent flow with distinct
responsibilities:
- **Agent 1 (Orchestrator)**: analyze source schema and propose gap operations
- **Agent 2 (Critic)**: audit and correct Agent 1 output with a reasoning model
- **Agent 3 (Sequence Planner)**: choose block order from the available pool

No agent may generate executable transformation code at runtime. Agent 3 MAY
reorder blocks, but it MUST NOT add or remove blocks from the available pool.

Rationale: explicit role boundaries keep LLM behavior auditable and prevent
architecture drift back to code generation.

### III. Declarative YAML Execution Only
Schema transformations MUST execute through declarative YAML mappings consumed by
`DynamicMappingBlock`. All supported primitives MUST compile to a known YAML
operation or to an explicit null/default fallback before the pipeline runs.
Runtime-generated Python transformation blocks are prohibited.

Generated mapping files MUST be written under
`src/blocks/generated/<domain>/DYNAMIC_MAPPING_<dataset>.yaml` and MUST be
treated as the source of truth for dataset-specific transformations.

Rationale: YAML-only execution provides deterministic behavior, reviewable
artifacts, and replay without sandbox risk.

### IV. Human Approval Gates
Human review MUST exist at the decision points that can materially change output
correctness:
- **Gate 1**: schema mapping review, including missing-column handling and
  schema exclusions
- **Gate 2**: quarantine review for rows that still fail required-field checks

There is no code-review gate for generated transforms because runtime code
generation is not allowed. Human decisions MUST be merged into the mapping state
before execution.

Rationale: these are the two points where operator intent changes the meaning of
the final dataset.

### V. Cascading Enrichment with Safety Boundaries
Enrichment MUST proceed in cost order:
1. `S1` deterministic extraction
2. `S2` KNN corpus search
3. `S3` RAG-assisted LLM categorization

`primary_category` MAY be resolved by `S1`, `S2`, or `S3`. `allergens`,
`dietary_tags`, and `is_organic` are safety fields and MUST remain
deterministic-only. They MUST NOT be inferred or modified by `S2` or `S3`.

Rationale: category tolerates probabilistic inference; safety fields do not.

### VI. Self-Extending Mapping Memory
When a dataset-specific mapping is generated, it MUST be persisted and
auto-discoverable on future runs. Re-ingesting a known source SHOULD reuse the
existing mapping artifact and avoid repeating schema-analysis work unless the
schema contract has changed.

Rationale: replayability and cost control are core behavior, not an optimization
detail.

### VII. Data Quality and Quarantine Enforcement
The pipeline MUST compute `dq_score_pre` before enrichment and `dq_score_post`
after pipeline execution. Rows that still fail required-field validation after
enrichment and alias application MUST be quarantined, and quarantine reasons
MUST be recorded in machine-readable form.

Output files written to `output/` MUST contain only rows that passed required
field validation unless a human explicitly overrides quarantine handling.

Rationale: output acceptance must be measurable and traceable.

## Technology Stack

- **Language**: Python 3.11
- **Data Processing**: pandas
- **LLM Access**: LiteLLM
- **Primary Models**: DeepSeek chat model for Agent 1 and Agent 3; reasoning
  model for Agent 2 when available
- **Workflow Engine**: LangGraph
- **UI**: Streamlit
- **Similarity Search**: FAISS

The constitution governs behavior, not vendor lock-in. Equivalent replacements
are allowed only if they preserve the agent responsibilities and constraints in
the Core Principles.

## Development Workflow

The default non-interactive graph MUST preserve this seven-node order:
1. `load_source`
2. `analyze_schema`
3. `critique_schema`
4. `check_registry`
5. `plan_sequence`
6. `run_pipeline`
7. `save_output`

The interactive Streamlit flow MUST expose the approval gates before execution
commits operator decisions to the YAML mapping or accepts quarantined results.

Quality gates for any feature or refactor:
- unified-schema alignment is documented
- YAML mapping behavior is explicit and testable
- enrichment safety fields remain deterministic-only
- replayed mappings under `src/blocks/generated/` still load correctly
- quarantine behavior and DQ scoring remain intact
- README, templates, and agent guidance stay consistent with the architecture

## Governance

This constitution supersedes conflicting local conventions and feature plans.
Every implementation plan, specification, task list, and runtime guidance
document MUST pass a constitution review before work is considered ready.

Amendments require:
- a written description of the rule change
- rationale for the change
- propagation to affected templates and guidance documents
- a semantic version update for this constitution

Versioning policy:
- **MAJOR**: removes a principle, redefines architecture boundaries, or changes a
  non-negotiable rule in a backward-incompatible way
- **MINOR**: adds a principle, adds a mandatory governance section, or materially
  expands implementation obligations
- **PATCH**: clarifies wording without changing required behavior

Compliance review expectations:
- plans MUST state how the work satisfies the constitution gates
- specs MUST capture schema, HITL, enrichment, and quarantine implications when
  relevant
- tasks MUST include the work needed to preserve YAML mappings, DQ logic, and
  documentation consistency
- runtime guidance MUST not describe deprecated architecture such as runtime
  code generation

**Version**: 1.3.0 | **Ratified**: 2026-04-17 | **Last Amended**: 2026-04-18
