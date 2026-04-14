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
For each unified schema column (excluding "computed" and "enrichment" columns), classify:
- MAP: A source column maps semantically. Provide source_column. Include in column_mapping.
- GAP: The unified column has a source equivalent but needs transformation (type change, format change). Include in gaps list with source_column set.

Return ONLY a JSON object with this exact structure:
{{
  "column_mapping": {{
    "source_col_name": "unified_col_name",
    ...
  }},
  "gaps": [
    {{
      "target_column": "unified_col_name",
      "target_type": "string",
      "source_column": "source_col_name",
      "source_type": "source_type",
      "action": "GAP",
      "sample_values": ["val1", "val2"]
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

Return ONLY a JSON object:
{{
  "column_mapping": {{
    "source_col": "unified_col_name",
    ...
  }},
  "dropped_columns": ["col1", "col2"],
  "gaps": []
}}"""


CODEGEN_PROMPT = """You are a code generation agent. Generate a Python transformation function.

## Gap to fill
- Target column: {target_column}
- Target type: {target_type}
- Source column: {source_column}
- Source type: {source_type}
- Sample source values: {sample_values}

## Constraints
- Function must be self-contained (no external dependencies beyond: re, pandas, datetime, math, json)
- Function signature: def transform_{target_column}(value):
- Return Python's None (NOT JavaScript's null) on failure or empty input
- Handle None/empty string inputs only
- Output type must match target_type: {target_type}
- Do NOT use: os, sys, subprocess, open, eval, exec, __import__
- Do NOT use JavaScript 'null'. Only Python's None is valid.
- Do NOT use bare except:. Use except Exception: instead.

## Return ONLY the Python function code, nothing else. No markdown, no explanation."""


CODEGEN_RETRY_PROMPT = """The previous function failed validation.

## Error
{error}

## Previous code
{previous_code}

## Original requirements
- Target column: {target_column}
- Target type: {target_type}
- Sample values: {sample_values}

Fix the function. Return ONLY the corrected Python function code."""
