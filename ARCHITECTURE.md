# Pipeline Architecture & Agent 3 Specification

## Current Architecture Overview

### Pipeline Flow

```
Input Schema → Agent 1 (Gap Analysis + HITL) → Agent 2 (Transformations/Blocks) 
→ Agent 3 (Sequence Generation) → Execution (with dedup) → Enrichment 
(keyword, semantic with sentence transformers, LLM)
```

### Agent Responsibilities

#### Agent 1: Orchestrator (Schema Analysis)

| Aspect | Details |
|--------|---------|
| **Input** | Source data schema |
| **LLM Call** | Yes — semantic mapping of source columns to unified schema |
| **Output** | `gaps`, `column_mapping`, `unified_schema` |
| **HITL** | User approves mappings before proceeding |

**Output Details:**
- `gaps`: Columns needing transformation (MAP, DROP, NEW, ADD)
- `column_mapping`: Source → unified column mapping
- `unified_schema`: Target schema with column specifications

---

#### Agent 2: Code Generator (Transformation Blocks)

| Aspect | Details |
|--------|---------|
| **Input** | Registry misses from Agent 1 |
| **LLM Call** | Yes — generates Python transform functions |
| **Output** | Validated `transform_<column>` functions saved to registry |

**Process Flow:**
1. Generate code via LLM
2. Sandbox validation (5s timeout, banned patterns check)
3. Retry up to 2 times if validation fails
4. Register validated functions

---

#### Agent 3: Sequence Generator *(Proposed)*

| Aspect | Details |
|--------|---------|
| **Input** | Data profile, schema gaps, domain, DQ pre-scores |
| **Decision Method** | Hybrid (rule-based + LLM for complex cases) |
| **HITL** | No — auto-execute decisions |

**Decision Inputs:**
- `source_schema`: What columns exist, data types, formats
- `unified_schema`: What fields are required in output
- `gaps`: What transformations are needed
- DQ pre-scores, null percentages, row count
- Domain (nutrition/safety/pricing)

**Rule-Based Decisions (Fast, No LLM Cost):**
| Condition | Action |
|-----------|--------|
| `row_count < 100` | Skip dedup (overhead not worth it) |
| `duplicate_ratio < 1%` (estimated) | Skip dedup |
| `product_name` column missing | Skip dedup |
| `domain == "pricing"` | Skip allergen extraction |
| All enrichment columns populated | Skip enrichment entirely |

**LLM-Powered Decisions (Complex Cases):**
- Which enrichment strategies to enable (S1/S2/S3)
- Dedup threshold tuning (stricter for safety, looser for pricing)
- Whether to include specific pre-processing blocks

**Output:**
```python
{
    "block_sequence": ["dq_score_pre", "__generated__", "strip_whitespace", ...],
    "enrichment_config": {
        "s1_enabled": True,   # Deterministic
        "s2_enabled": True,   # Semantic KNN
        "s3_enabled": False   # LLM (skipped for this data)
    },
    "skip_reasons": {
        "fuzzy_deduplicate": "Duplicate ratio < 1%, skipping",
        "llm_enrich.s3": "Corpus too small (< 10 rows)"
    }
}
```

---

## Block Sequence

### Default Sequence (Nutrition/Safety Domain)

```
1.  dq_score_pre          → Calculate pre-enrichment DQ score
2.  __generated__        → LLM-generated transform functions (injected)
3.  strip_whitespace      → Clean whitespace
4.  lowercase_brand       → Brand name normalization
5.  remove_noise_words    → Remove noise patterns
6.  strip_punctuation    → Remove punctuation
7.  extract_quantity_column → Parse quantity (or keep_quantity_in_name for pricing)
8.  extract_allergens    → FDA Big-9 allergen extraction (nutrition/safety only)
9.  fuzzy_deduplicate    → Blocking + fuzzy matching + clustering
10. column_wise_merge     → Merge across duplicate clusters
11. golden_record_select → Select best record per cluster
12. llm_enrich           → 3-strategy enrichment
13. dq_score_post        → Calculate post-enrichment DQ score + delta
```

### Agent 3-Generated Sequence (Proposed)

Instead of running all 13 blocks, Agent 3 dynamically generates the sequence based on data profile.

**Example Optimized Sequences:**

*High-quality data (skip enrichment):*
```
dq_score_pre → __generated__ → strip_whitespace → fuzzy_deduplicate → ...
(llm_enrich skipped because all fields populated)
```

*Small dataset (skip dedup):*
```
dq_score_pre → __generated__ → strip_whitespace → llm_enrich → dq_score_post
(fuzzy_deduplicate skipped, row_count < 100)
```

*Priciing domain (skip allergens):*
```
dq_score_pre → __generated__ → strip_whitespace → ... → keep_quantity_in_name → 
llm_enrich → dq_score_post
(extract_allergens skipped, domain == "pricing")
```

---

## Deduplication Process

### Current Implementation

| Step | Description |
|------|-------------|
| **1. Blocking** | First 3 chars of `product_name` (lowercased) → dict of `block_key → [row_indices]` |
| **2. Pairwise Scoring** | Within each block: RapidFuzz `token_sort_ratio` + `ratio` on name/brand/combined, weighted sum |
| **3. Union-Find Clustering** | Transitive closure — if A~B and B~C, all 3 in same cluster |
| **4. Mark Canonical** | First row in each cluster = `True`, rest = `False` |

### Algorithm Details

**Weights:**
- `name_weight`: 0.5 (product_name similarity)
- `brand_weight`: 0.2 (brand_name similarity)
- `combined_weight`: 0.3 (name + brand combined)

**Threshold:** 85 (configurable)

**Output Columns:**
- `duplicate_group_id`: Cluster assignment
- `canonical`: True for first row in each cluster

### Optimization Considerations

| Issue | Current State | Potential Improvement |
|-------|--------------|----------------------|
| Blocking key | 3-char prefix only | Double blocking (2+3 char), soundex, n-gram |
| Within-block comparison | O(n²) pairwise | Parallel processing, MinHash LSH for large blocks |
| False negatives | Products with different first 3 chars not matched | Word token blocking, semantic embeddings |
| Scale | Works well for < 10k rows | Consider sorted neighborhood or LSH for > 10k |

### Agent 3 Dedup Decisions (Proposed)

```python
# Rule-based skips
if row_count < 100:
    skip "fuzzy_deduplicate"
if "product_name" not in df.columns:
    skip "fuzzy_deduplicate"
if estimated_duplicate_ratio < 0.01:
    skip "fuzzy_deduplicate"

# Also skip downstream blocks that depend on dedup
if "fuzzy_deduplicate" skipped:
    skip "column_wise_merge"
    skip "golden_record_select"
```

---

## Enrichment Strategies

### Architecture: 3-Tier Cascade

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           llm_enrich Block                               │
│                                                                          │
│  needs_enrichment = rows with null in [primary_category, allergens,      │
│                                       dietary_tags, is_organic]         │
│                                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐   │
│  │    S1       │ →  │    S2       │ →  │         S3                 │   │
│  │Deterministic│    │ KNN + FAISS │    │ RAG-Augmented LLM          │   │
│  │ Extraction  │    │ Semantic    │    │ (DeepSeek)                  │   │
│  └─────────────┘    └─────────────┘    └─────────────────────────────┘   │
│       ↓                  ↓                        ↓                      │
│  allergens, is_organic, dietary_tags, primary_category (if matched)     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Strategy 1: Deterministic Extraction

**Approach:** Regex + keyword matching

**Rules:**
- **Category:** Keywords in product_name/ingredients/category
  - `(cereal|oat|granola)` → "Breakfast Cereals"
  - `(milk|cream|yogurt|cheese)` → "Dairy"
  - ... 20+ categories
- **Dietary tags:** Explicit label claims in product_name (NOT ingredients)
  - `(gluten[\s-]?free)` → "gluten-free"
  - `(vegan)` → "vegan"
- **is_organic:** "organic" or "usda organic" keyword scan
- **allergens:** FDA Big-9 regex patterns

**Safety:** allergens, is_organic, dietary_tags are EXTRACTION-ONLY (never passed to S2/S3)

---

### Strategy 2: Semantic Search (KNN + FAISS)

**Approach:** Sentence Transformers + FAISS vector database

**Process:**
1. **Seed corpus** — Build FAISS index from S1-resolved rows
2. **KNN search** — Query with `all-MiniLM-L6-v2` embeddings
3. **Vote** — Weighted majority vote from K=5 neighbors
4. **Confidence threshold** — >= 0.60 to accept; else escalate to S3

**Configuration:**
- `K_NEIGHBORS`: 5
- `VOTE_SIMILARITY_THRESHOLD`: 0.45
- `CONFIDENCE_THRESHOLD_CATEGORY`: 0.60
- `MIN_CORPUS_SIZE`: 10

**RAG Context:** Top-3 neighbors stored in `_knn_neighbors` column (JSON) for S3 prompt injection

---

### Strategy 3: RAG-Augmented LLM

**Approach:** DeepSeek with KNN neighbors as context

**Prompt Construction:**
```
Similar products already categorized:
  - Cheerios + Milk → Dairy (similarity: 0.87)
  - Honey Nut Oats → Breakfast Cereals (similarity: 0.82)

Product to categorize:
  Name: Honey Nut Cheerios
  Brand: General Mills
  Ingredients: Whole Grain Oats, Honey, Salt...
  
What is the primary_category?
```

**Safety:** Only operates on `primary_category`. allergens/is_organic/dietary_tags stay null if S1 fails.

**Feedback Loop:** High-confidence S3 results added to corpus for future KNN.

---

### Agent 3 Enrichment Decisions (Proposed)

```python
# Rule-based
if all enrichment columns populated:
    skip "llm_enrich" entirely

# LLM-powered (analyze data characteristics)
if corpus_size < 10:
    disable S2 (semantic)
    enable S3 (LLM fallback)
    
if low keyword density:
    disable S1 (deterministic won't help)
    enable S2 + S3
    
if domain == "safety":
    enable S1 (deterministic critical for allergens)
    enable S2 + S3 (maximize coverage)
```

---

## DQ Score Timeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          BLOCK SEQUENCE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Step 1: DQScorePreBlock              Step 13: DQScorePostBlock         │
│  ┌────────────────────┐               ┌────────────────────┐             │
│  │ df["dq_score_pre"] │   ...         │ df["dq_score_post"]│            │
│  │ = compute_dq_score │               │ = compute_dq_score│             │
│  └────────────────────┘               └────────────────────┘             │
│          │                                     │                          │
│          └──────────── delta computed ─────────┘                          │
│                      in post block:                                      │
│                      df["dq_delta"] =                                    │
│                        dq_score_post - dq_score_pre                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### DQ Score Formula

```
DQ Score = Completeness * 0.4 + Freshness * 0.35 + IngredientRichness * 0.25

Where:
- Completeness: fraction of non-null values per row
- Freshness: normalized published_date (if exists)
- IngredientRichness: normalized ingredients text length
```

### Delta Evaluation

**Location:** `src/blocks/dq_score.py:81` (DQScorePostBlock.run())

```python
if "dq_score_pre" in df.columns:
    df["dq_delta"] = (df["dq_score_post"] - df["dq_score_pre"]).round(2)
```

---

## HITL Gates

| Gate | Location | Action |
|------|----------|--------|
| **HITL 1** | Step 1 → Step 2 | "Approve Mapping & Continue" |
| **HITL 2** | Step 2 | Code review + "Approve & Register" or "Regenerate Failed" |
| **HITL 3** | Step 4 (Results) | "Accept Quarantine" or "Override: Include All" |

---

## File Structure

```
src/
├── agents/
│   ├── orchestrator.py      # Agent 1: Schema analysis, gap detection
│   ├── code_generator.py     # Agent 2: LLM code generation, sandbox validation
│   ├── sequence_generator.py # Agent 3: Sequence generation (PROPOSED)
│   ├── graph.py              # LangGraph state machine + step-by-step runner
│   ├── state.py              # PipelineState TypedDict schema
│   └── prompts.py            # LLM prompt templates
├── blocks/
│   ├── base.py                # Abstract Block base class
│   ├── fuzzy_deduplicate.py   # Deduplication logic
│   ├── llm_enrich.py          # 3-strategy enrichment orchestrator
│   ├── column_wise_merge.py   # Merge across duplicate clusters
│   ├── golden_record_select.py # Select best record per cluster
│   ├── dq_score.py            # Pre/post DQ scoring
│   └── ...
├── enrichment/
│   ├── deterministic.py       # S1: Regex/keyword extraction
│   ├── embedding.py           # S2: KNN corpus search
│   ├── llm_tier.py            # S3: RAG-augmented LLM
│   └── corpus.py              # FAISS index management
├── registry/
│   ├── block_registry.py       # Pre-built transformation blocks
│   └── function_registry.py    # LLM-generated functions storage
├── schema/
│   └── analyzer.py             # DataFrame profiling, schema diff
└── models/
    └── llm.py                  # LiteLLM wrapper (DeepSeek)
```

---

## Agent 3 Integration Point

### Graph Modification

```
load_source → analyze_schema → check_registry
                                           ↓
              ┌────────────────────────────┴────────────────────────────┐
              ↓                                                         ↓
        [registry hits only]                                     [registry misses]
              ↓                                                         ↓
         run_pipeline ───────────────────────────→ generate_code → validate_code
                                                                         ↓
                                                     ┌────────────────────┴────────────────────┐
                                                     ↓                                         ↓
                                              [all passed?]                                 [retry]
                                                     ↓                                         ↓
                                          register_functions ────────────→ generate_code
                                                                         │
                                                         ┌───────────────┴───────────────┐
                                                         ↓                               ↓
                                                  [Agent 3: Sequence Generator]     [END if no code]
                                                         ↓
                                                  run_pipeline (custom seq)
                                                         ↓
                                                  save_output → END
```

### State Changes

```python
# src/agents/state.py additions
class PipelineState(TypedDict, total=False):
    # ... existing fields ...
    
    # New field for Agent 3
    enrichment_config: dict  # {"s1_enabled": bool, "s2_enabled": bool, "s3_enabled": bool}
```

---

## Open Questions

1. **Dedup threshold tuning** — Should Agent 3 also tune the 85 threshold dynamically? (stricter for safety, looser for pricing)

2. **Dedup before or after enrichment?** — Currently dedup runs BEFORE enrichment. Does that make sense?
   - Before: Fewer rows to enrich, cheaper
   - After: Better canonical selection if you know the category first

3. **Downstream block dependencies** — `column_wise_merge` and `golden_record_select` only make sense if dedup ran. Should Agent 3 skip these automatically if dedup is skipped?

4. **Rollback path** — If Agent 3 skips dedup but user disagrees, is there a path to re-run just dedup without full pipeline?
