# 4-Tier Data Enrichment Pipeline - Summary

## Overview

The conversation explored how the ETL pipeline handles data enrichment for null values across 4 tiers, with specific focus on when each tier succeeds or fails, and proposed improvements for Tier 2 and Tier 3.

---

## Current Enrichment Architecture

### Enrichment Columns (Pre-defined)

The system enriches only these columns (hardcoded in `src/blocks/llm_enrich.py:16`):

```python
ENRICHMENT_COLUMNS = ["primary_category", "dietary_tags", "is_organic", "allergens"]
```

These can be overridden via config at runtime.

---

## Tier-by-Tier Breakdown

### Tier 1: Deterministic (Regex/Keyword Matching)

**How it works**: Scans `product_name`, `ingredients`, and `category` columns against hardcoded regex patterns and keyword rules.

**Data source**: Hardcoded rules in `src/enrichment/deterministic.py`

**Resolution logic**:
- Primary category: Match keywords like "milk", "cheese" → "Dairy"
- Dietary tags: Match "gluten-free", "vegan" → add to dietary_tags list
- is_organic: Match "organic" keyword → set to True

**Pass examples**:
- "Organic Greek Yogurt" → primary_category: "Dairy", is_organic: true
- "Gluten-Free Almond Flour" → dietary_tags: "gluten-free"

**Fail examples**:
- "365 Everyday Value Organic" (no product type keywords)
- "Queso Fresco" (Spanish terms not in regex)

---

### Tier 2: Embedding Similarity

**How it works**: 
1. Encode row text (product_name, ingredients) into embeddings using sentence-transformers (`all-MiniLM-L6-v2`)
2. Compare against hardcoded `CATEGORY_TAXONOMY` list
3. If similarity ≥ 0.4, assign best matching category

**Data source**: Static taxonomy in `src/enrichment/embedding.py:13-18`:
```python
CATEGORY_TAXONOMY = [
    "Breakfast Cereals", "Dairy", "Meat & Poultry", "Seafood", "Bakery",
    "Confectionery", "Snacks", "Beverages", "Condiments", "Frozen Foods",
    "Fruits", "Vegetables", "Pasta & Grains", "Soups", "Baby Food",
    "Organic", "Supplements", "Canned Foods", "Deli", "Pet Food",
]
```

**Key limitation**: Single-word taxonomy is insufficient for语义 diversity.

**Pass examples**:
- "Red Lentil Pasta" → similarity 0.72 to "Pasta & Grains" → assigned
- "Frozen Mixed Vegetables" → similarity 0.65 to "Vegetables" → assigned

**Fail examples**:
- "Daily Harvest Smoothie" → similarity 0.35 (below 0.4 threshold)
- "Primal Kitchen Mayo" → brand-heavy, low similarity 0.28

---

### Tier 3: Cluster Propagation

**How it works**: 
- Requires `duplicate_group_id` column from prior deduplication step
- Within each duplicate group, if some rows have values and others don't, propagate the most common non-null value to null rows

**Data source**: `duplicate_group_id` column must exist in the dataframe

**Pass examples**:
- Cluster grp_001: Row A has primary_category="Beverages", Row B and C are null → B and C inherit "Beverages"

**Fail examples**:
- No `duplicate_group_id` column in data → tier skipped entirely
- Cluster where all rows are null → nothing to propagate

---

### Tier 4: LLM (GROQ)

**How it works**: 
- Only invoked for rows still null after Tiers 1-3
- Batches rows in groups of 5 to reduce API calls
- Sends product data (product_name, brand_name, category, ingredients) to LLM
- LLM fills only the missing fields

**Data source**: GROQ API via `src/models/llm.py`

**Prompt**: Structured to ask for specific fields only (primary_category, dietary_tags, is_organic, allergens)

**Pass examples**:
- "Trader Joe's Organic Almond Butter" → sufficient context to infer Dairy category
- "Beyond Meat Burger" → can identify allergens correctly

**Fail examples**:
- "Product X" (minimal context) → LLM hallucinates
- "随机中文标签" (non-English) → LLM fails to parse

---

## Identified Issues and Proposed Improvements

### Issue 1: T2 Static Taxonomy is Too Limited

**Problem**: Only 20 hardcoded categories, single-word matching doesn't capture semantic diversity.

**Proposed Improvement - SLM + Dynamic Taxonomy**:

1. **Use SLM instead of LLM**: 
   - Task is simple keyword extraction → SLM (Phi-3, TinyLlama, Mistral-7B) is sufficient
   - Cost: ~50-200ms latency vs LLM's 500ms+
   - Local deployment, no API calls

2. **Exemplar-based taxonomy** instead of single words:
   ```python
   CATEGORY_TAXONOMY = {
       "Dairy": ["milk", "yogurt", "cheese", "butter", "cream", "queso", "paneer", "ghee"],
       "Snacks": ["chips", "pretzels", "crackers", "bars", "nuts"],
   }
   ```
   - Row text compared against multiple exemplars per category
   - Higher recall for international products ("Queso fresco", "Paneer")

3. **Dynamic expansion**:
   - Agent samples incoming rows with known categories
   - Extracts canonical category names using SLM
   - Adds novel categories to taxonomy
   - Re-runs embedding similarity with expanded taxonomy

---

### Issue 2: T3 Depends on Pre-existing Structure

**Problem**: 
- Requires `duplicate_group_id` from prior deduplication step
- Not true "enrichment" — more like cluster propagation
- If no deduplication ran, or no duplicates exist, T3 does nothing

**Proposed Solutions**:

1. **Ad-hoc clustering**: Group by embedding similarity (reuse T2 logic), then propagate within those clusters

2. **Rename/reposition**: Move T3 to "deduplication post-processing" rather than enrichment tier

3. **Self-propagation**: If 2+ rows share high embedding similarity (>0.8), treat as cluster and propagate

---

## Key Insights from Conversation

1. **Enrichment columns are pre-defined** at code level (not dynamic per-run unless overridden via config)

2. **T2 is NOT row-to-row matching** — it's row text → taxonomy matching (misconception corrected)

3. **T3 is NOT category-based grouping** — it's duplicate cluster propagation using pre-existing `duplicate_group_id`

4. **LLM calls are last-resort** — only for rows truly ambiguous after tiers 1-3, minimizing API cost

5. **Hallucination risk in T4** — more missing fields = higher chance LLM fails

6. **SLM sufficient for T2** — task is simple keyword extraction, not complex reasoning

7. **Taxonomy as exemplars** is better than single words for semantic matching

---

## File References

- `src/blocks/llm_enrich.py` - Main orchestration block (lines 16, 26-80)
- `src/enrichment/deterministic.py` - Tier 1 (lines 10-42, 47-97)
- `src/enrichment/embedding.py` - Tier 2 (lines 13-18, 44-89)
- `src/enrichment/propagation.py` - Tier 3 (lines 8-43)
- `src/enrichment/llm_tier.py` - Tier 4 (lines 14-92)

---

## Summary Table: Pass/Fail by Tier

| Tier | Pass Criteria | Fail Criteria |
|------|---------------|---------------|
| **T1** | Clear keyword matches in product_name/ingredients | Ambiguous names, misspellings, non-English |
| **T2** | Similarity ≥ 0.4 to taxonomy | Vague names, brand-heavy, novel products |
| **T3** | `duplicate_group_id` exists + partial cluster fill | No column, or all-null clusters |
| **T4** | Sufficient context for inference | Minimal info (hallucination risk) |