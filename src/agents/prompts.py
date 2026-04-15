"""Prompt templates for Agent 1 (Orchestrator) and Agent 2 (Code Generator)."""

SCHEMA_ANALYSIS_PROMPT = """You are a schema analysis agent for a data enrichment pipeline.

You are given:
1. An incoming data source's schema (column names, types, sample values, null rates)
2. A unified output schema that all data sources must conform to

Your task: For each column in the unified schema, determine how to map it from the incoming source.

## Incoming Source Schema
{source_schema}

## Unified Output Schema
{unified_schema}

## Semantic Mapping Examples
Map source columns to unified columns based on SEMANTIC meaning, not just name:
- "product_description" → "product_name" (both describe product name)
- "item_name" → "product_name"
- "name" → "product_name"
- "recalling_firm" → "brand_owner" (firm that owns/recalls the product)
- "manufacturer" → "brand_owner"
- "brand" → "brand_name"
- "category" → "category" (same meaning)
- "product_type" → "category" (type is a category)
- "recall_initiation_date" → "published_date" (date field)
- "report_date" → "published_date" (date field)
- "code_info" → "data_source" (code/source info)
- "event_id" → "data_source" (ID as source reference)

## Instructions
For each unified schema column (excluding "computed" and "enrichment" columns), classify into one of four categories:

1. **MAP**: A source column maps semantically with no transformation needed. Include in column_mapping.
2. **TYPE_CAST**: A source column maps semantically but needs a type conversion (e.g., int64 → string, object → float). Include in derivable_gaps with action "TYPE_CAST".
3. **DERIVE**: The unified column can be computed from one or more existing source columns via transformation logic (e.g., parsing a description, combining fields). Include in derivable_gaps with action "DERIVE".
4. **MISSING**: No source column exists and the column CANNOT be derived from any combination of source columns. The data simply does not exist in this source. Include in missing_columns with a reason explaining why.

CRITICAL: If a unified column has no plausible source column AND cannot be derived from any combination of source columns, classify it as MISSING — do NOT classify it as a gap with a null source_column. The pipeline cannot invent data that does not exist.

Return ONLY a JSON object with this exact structure:
{{
  "column_mapping": {{
    "source_col_name": "unified_col_name",
    ...
  }},
  "derivable_gaps": [
    {{
      "target_column": "unified_col_name",
      "target_type": "string",
      "source_column": "source_col_name",
      "source_type": "source_type",
      "action": "TYPE_CAST",
      "sample_values": ["val1", "val2"]
    }}
  ],
  "missing_columns": [
    {{
      "target_column": "unified_col_name",
      "target_type": "string",
      "reason": "Source dataset does not contain this information"
    }}
  ]
}}

For enrichment/computed columns (allergens, primary_category, dietary_tags, is_organic, dq_score_*, dq_delta), skip entirely — they are handled downstream."""


FIRST_RUN_SCHEMA_PROMPT = """You are a schema analysis agent. This is the FIRST data source for this pipeline.
There is no unified schema yet — you must derive one.

## Incoming Source Schema
{source_schema}

## Domain: {domain}

## Instructions
Analyze the source columns and create a column mapping from source names to clean unified names.

Rules:
- Rename columns to clean, standardized names (e.g., "brand_owner" -> "brand_name", "description" -> "product_name")
- Drop columns that are metadata/IDs not useful for the product catalog (e.g., "fdc_id", "gtin_upc")
- Keep columns relevant to product identity: name, brand, category, ingredients, serving info
- NEVER map nutrient/nutrition measurement columns (e.g., "foodNutrients", "nutrients") to "ingredients". Nutrient arrays contain lab measurements (Protein, Fat, Vitamins), not ingredient lists. If no true ingredients text column exists, leave "ingredients" unmapped.

Return ONLY a JSON object:
{{
  "column_mapping": {{
    "source_col": "unified_col_name",
    ...
  }},
  "dropped_columns": ["col1", "col2"],
  "gaps": [
    // Leave empty on first run unless a source column requires type coercion to fit the unified name
  ]
}}"""


SEQUENCE_PLANNING_PROMPT = """You are a pipeline sequence planner for a data enrichment ETL system.

You are given a set of pipeline blocks that MUST ALL run. Your task is to determine the optimal execution order.

## Domain
{domain}

## Source Schema (column names and types)
{source_schema}

## Schema Gaps and Registry Results
{gap_summary}

## Available Blocks (all must appear exactly once in your output)
{blocks_metadata}

## Ordering Rules
- dq_score_pre MUST be first
- dq_score_post MUST be last
- Normalization blocks (strip_whitespace, lowercase_brand, remove_noise_words, strip_punctuation) must run before deduplication
- extract_allergens must run before llm_enrich
- Deduplication blocks (fuzzy_deduplicate, column_wise_merge, golden_record_select) must run after normalization
- llm_enrich must run after deduplication
- __generated__ (dynamically generated schema transformation blocks) should run after dq_score_pre but before normalization blocks
- Use stage names: "dedup_stage" expands to [fuzzy_deduplicate, column_wise_merge, golden_record_select]
- Use stage names: "enrich_stage" expands to [extract_allergens, llm_enrich]

## Stage Expansion
- dedup_stage = ["fuzzy_deduplicate", "column_wise_merge", "golden_record_select"]
- enrich_stage = ["extract_allergens", "llm_enrich"]

Return ONLY a JSON object with this exact structure:
{{
  "block_sequence": ["block_name_1", "block_name_2", ...],
  "reasoning": "One sentence explaining the key ordering decision made"
}}

Include every block from the input list exactly once. Do not add or remove any blocks.
You may use stage names (dedup_stage, enrich_stage) or expand them — either is valid."""


CODEGEN_PROMPT = """You are a code generation agent. Generate a Python Block class for a complex schema transformation.

NOTE: Simple operations (type casts, null column creation, format transforms) are handled declaratively via YAML.
You are only called for DERIVE gaps — columns that must be computed from one or more existing source columns using non-trivial logic.

## Gap to fill
- Target column: {target_column}
- Target type: {target_type}
- Source column(s): {source_column}
- Source type: {source_type}
- Sample source values: {sample_values}
- Domain: {domain}
- Dataset name: {dataset_name}

## Block Template to Follow
```python
import pandas as pd
from src.blocks.base import Block


class {block_name}Block(Block):
    name = "{block_name}"
    domain = "{domain}"
    description = "Auto-generated: {description}"
    inputs = {input_cols}
    outputs = {output_cols}

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # TODO: Implement derivation logic
        return df
```

## Safe NA Patterns (required — these prevent runtime TypeErrors)
- Float/int creation: `df['col'] = float('nan')` — NEVER `pd.NA` then `.astype('float64')`
- Numeric conversion: `pd.to_numeric(df['src'], errors='coerce')` — NEVER `.astype('float64')` directly
- String cast: `.astype(str)` is safe but NEVER cast None to str (produces literal "None")
- Boolean: `df['col'] = None`

## Constraints
- Block must inherit from src.blocks.base.Block
- Must implement run(self, df, config=None) -> pd.DataFrame
- Use df.copy() to avoid modifying original
- Handle None/NA values gracefully
- Do NOT use: os, sys, subprocess, open, eval, exec, __import__

## Naming Convention
Block name format: DERIVE_{{TargetColumn}}_{{DatasetName}}

## Return ONLY the Python Block class code, nothing else. No markdown, no explanation."""


CODEGEN_RETRY_PROMPT = """The previous Block class failed validation.

## Error
{error}

## Previous code
{previous_code}

## Original requirements
- Target column: {target_column}
- Target type: {target_type}
- Source column: {source_column}
- Source type: {source_type}
- Sample values: {sample_values}
- Domain: {domain}

## Fix the Block class. Return ONLY the corrected Python Block class code."""
