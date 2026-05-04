# 🛒 Marketplace Intelligence & Data Quality Observability Platform

> **Production-Grade Schema-Driven ETL with 3-Agent LangGraph + Data Quality Observability**
> DAMG 7245: Big Data and Intelligent Analytics — Spring 2026 

Every major marketplace company faces the same problem: product catalog data arrives from dozens of sources with zero standardization. DoorDash ingests restaurant menus with abbreviations and typos. Walmart merges supplier feeds with completely different schemas. Airbnb receives property descriptions in dozens of languages. The result is a catalog where the same product appears differently depending on where the data came from — breaking search, silently degrading quality, and requiring manual engineering work every time a new source is added.

MIP is a production-grade data platform that eliminates that manual work. A three-agent LangGraph flow detects schema gaps against a canonical per-domain schema, synthesizes declarative YAML transforms, and replays them deterministically on subsequent runs. A composable block engine handles cleaning, fuzzy deduplication, cascading enrichment, and DQ scoring — with a full Data Quality Observability stack (Prometheus, Pushgateway, Grafana, Postgres audit, ChromaDB RAG chatbot) layered on top so every run is traceable, queryable, and anomaly-aware.

**New datasets onboard with zero hand-written Python — agents draft, critique, and sequence YAML transforms; humans approve via HITL gates.**

---

## 🔗 Links

| Resource | URL |
|----------|-----|
| **Streamlit App (Live)** | [marketplace-intelligence-platform.streamlit.app](https://marketplace-intelligence-platform.streamlit.app/) |
| **Grafana Observability Dashboard** | [ETL Pipeline Observability Snapshot](https://etlobservability.grafana.net/dashboard/snapshot/bkH00mlIW8VDwLulhVmpaeCw4qRHjk4F) |
| **GitHub Repository** | [BigDataIA-Spring26-MIP/Marketplace-Intelligence-Platform](https://github.com/BigDataIA-Spring26-MIP/Marketplace-Intelligence-Platform) |
| **Project Codelabs** | [MIP Walkthrough](https://codelabs-preview.appspot.com/?file_id=1qG3yRCYtzhSTFo97FNTSXtc9i-zMtj7WOxIXy7K4dWI#0) |
| **Architecture Diagram** | [Eraser Architecture Diagram](https://app.eraser.io/workspace/CjaYXVMjanRfc6Y5vVRI) |

---

## 📋 Table of Contents

1. [Project Overview & Context](#1-project-overview--context)
2. [Objective & Business Problem](#2-objective--business-problem)
3. [Architecture](#3-architecture)
4. [Phase Progression](#4-phase-progression)
5. [Pipeline Flows](#5-pipeline-flows)
6. [CLI & Streamlit Dashboard](#6-cli--streamlit-dashboard)
7. [Setup & Installation](#7-setup--installation)
8. [Summary & Key Takeaways](#8-summary--key-takeaways)
9. [Design Decisions & Tradeoffs](#9-design-decisions--tradeoffs)
10. [Known Limitations](#10-known-limitations)

---

## 1. 🌐 Project Overview & Context

This project was built by studying 20 engineering blog posts from companies that solve these problems in production — DoorDash (AutoEval, knowledge graphs for search), Airbnb (Data Quality Score, dataset trust), Netflix (statistical anomaly detection across hundreds of metrics), and Walmart (entity resolution across sources with no universal product ID). No single company builds all of these capabilities in one system. MIP combines catalog intelligence, data quality observability, and automated enrichment into one connected platform where each layer feeds the others.

The domain is grocery and food products — because this category has the richest open datasets available (over 44 million records across six sources, ~10.5 GB) — but every technique applies to any product catalog at any marketplace.

Before MIP, onboarding a new source required a developer to manually read the source schema, write per-source Python transforms, and wire up ad-hoc quarantine rules — with no systematic way to detect missing fields and no observability on silent failures. The five-phase build progression is detailed in [Section 4](#4--phase-progression).

---

## 2. 🎯 Objective & Business Problem

The same product appears across four real data sources as: "Cheerios" (OpenFoodFacts), "Cheerios Cereal by General Mills" (USDA), "General Mills CHEERIOS Honey Nut 10.8oz" (FDA Recall Database), and "Cheerios in the breakfast department" (Instacart). Four naming conventions. No shared product ID. No standardized attribute format. This is the exact challenge described in engineering blogs by DoorDash, Uber, Walmart, and Airbnb.

This fragmentation causes two downstream failures. First, search breaks — when a customer types "gluten-free breakfast cereal," the system returns nothing because the catalog has no structured dietary tags and keyword matching fails on unstructured descriptions. Second, data quality silently degrades — a source changes its format overnight, null rates spike from 2% to 40%, and nobody notices until an executive sees a wrong number in a report.

MIP solves five problems simultaneously:

1. **Hand-written transforms** — a new source should onboard by pointing the CLI at it and confirming the YAML the agents produce. Zero lines of Python by hand.
2. **Unified downstream schema** — every row landing in Gold has the same 14-column canonical shape regardless of its Bronze origin.
3. **Auditability** — YAML-only transforms mean no `exec()`, generated files are diffable in git, and every block logs `rows_in/rows_out` to an audit trail.
4. **Cost-aware enrichment** — cheap deterministic rules run first, FAISS KNN second, LLM only for rows that remain uncategorized, with top-3 neighbors passed as RAG context.
5. **Observability without blocking** — observability emits are wrapped in `try/except`; an outage in the observability plane never fails an ETL run.

### Scope

| Phase | Detail |
|:---|:---|
| Source Ingestion | USDA, OFF, openFDA, ESCI, Open Prices — Kafka producers or direct CLI; Bronze JSONL to GCS |
| Schema Analysis | 3-agent LangGraph flow; YAML transform generation; schema fingerprint caching |
| Block Execution | 13 static blocks + dynamic mapping; chunked streaming at 10K rows |
| Enrichment | S1 deterministic → S2 FAISS KNN → S3 LLM-RAG (category only) |
| Medallion Storage | Bronze JSONL, Silver Parquet, Gold BigQuery — all partitioned by source + date |
| Orchestration | Airflow DAGs: incremental ingest, Bronze→BQ, Bronze→Silver, Silver→Gold, hourly anomaly, 5-min chunker |
| Observability | Prometheus, Pushgateway, Grafana, Postgres audit, ChromaDB RAG, MCP FastAPI server, Isolation Forest anomaly |
| HITL UI | Streamlit wizard with 7-step gates + Observability chatbot |

### Target Data Sources

| Source | Records | Domain | License |
|:---|:---|:---|:---|
| USDA FoodData Central | ~465k | nutrition | Public domain |
| Open Food Facts (OFF) | ~1M | nutrition / retail | ODbL v1.0 |
| openFDA Food Enforcement | ~25k | safety | Public domain |
| Amazon ESCI | ~2M | retail / search | Apache 2.0 |
| Open Prices | Growing, Europe-weighted | pricing | ODbL |
| Instacart | ~3M orders | retail / basket | Non-commercial |

---

## 3. 🏗️ Architecture

### Architecture Diagram

[MIP Architecture Diagram](https://app.eraser.io/workspace/CjaYXVMjanRfc6Y5vVRI)

MIP follows a medallion architecture (Bronze → Silver → Gold) layered over a three-agent LangGraph control plane and a Data Quality Observability plane. The two planes are deliberately decoupled: observability emits are best-effort, wrapped in `try/except`, and an observability outage never fails an ingestion run.

**Layer 1 — Source & Ingestion:** Kafka producers (`src/producers/openfda_producer.py`, `off_producer.py`) stream source records to the `pipeline.events` topic; `src/consumers/kafka_gcs_sink.py` flushes JSONL to `gs://mip-bronze-2024/` in fixed-size batches. The CLI path bypasses Kafka for local CSVs.

**Layer 2 — LangGraph Control Plane (3 Agents, 7 Nodes):** `src/agents/graph.py` builds a `StateGraph`:
```
load_source → analyze_schema → [critique_schema?] → check_registry → plan_sequence → run_pipeline → save_output
```
- **Agent 1** (`analyze_schema`) — maps source → unified schema; emits `RENAME/CAST/FORMAT/DELETE/ADD/SPLIT/UNIFY/DERIVE` ops.
- **Agent 2** (`critique_schema`) — off by default, enabled with `--with-critic`. Skipped when Redis returns a cached YAML.
- **Agent 3** (`plan_sequence`) — reorders blocks; cannot add or remove. Any block the LLM drops is re-appended automatically before `dq_score_post`.

**Layer 3 — Block Execution Engine:** 13 static blocks (cleaning, fuzzy dedup, golden-record selection, enrichment, DQ scoring) plus dynamic `DynamicMappingBlock` instances generated per source. `PipelineRunner.run_chunked()` iterates the sequence at 10K rows/chunk.

**Layer 4 — Medallion Storage:**
- Bronze: `gs://mip-bronze-2024/` — JSONL, partitioned by source + date
- Silver: `gs://mip-silver-2024/` — Parquet, schema-conformed, partitioned by domain + source
- Gold: BigQuery `mip_gold.products` — deduped, enriched, DQ-scored, append-mode

**Layer 5 — Data Quality Observability Plane:**
- `RunLogWriter` — atomic JSON run logs per pipeline run
- `MetricsExporter` — pushes 12 labelled Prometheus gauges to Pushgateway (`:9091`)
- `AnomalyDetector` — Isolation Forest on the last N runs per source; writes `anomaly_reports` to Postgres
- `chunker.py` — 5-minute loop pulling new `audit_events` rows into ChromaDB `audit_corpus`
- `kafka_to_pg.py` — demuxes `pipeline.events` into 4 Postgres tables
- `mcp_server.py` — FastAPI on `:8001` with 7 tool endpoints, Redis-cached (15s TTL Prometheus, 30s Postgres)
- `ObservabilityChatbot` — RAG over run logs, returns answer + cited run IDs

**Cache Layer:** Redis (with SQLite fallback at `output/cache.db`) keyed on schema fingerprint — a complete YAML cache hit short-circuits Agents 1, 2, and 3.

### Tech Stack

| Category | Technology | Version | Purpose |
|:---|:---|:---|:---|
| Runtime | Python | ^3.11 (Poetry) | Application runtime |
| Agent Framework | LangGraph | ^0.4 | State machine for 3-agent flow |
| LLM Router | LiteLLM | ^1.55 | Multi-provider LLM abstraction |
| Dataframe Engine | pandas | ^2.2 | In-chunk transformations |
| Vector Index | FAISS-CPU | ^1.8 | KNN for category enrichment (S2) |
| Embeddings | sentence-transformers | ^3.0 | all-MiniLM-L6-v2 for S2 enrichment + observability audit chunker |
| Fuzzy Matching | rapidfuzz | ^3.9 | Dedup cluster keys |
| Columnar Format | pyarrow | ^16 | Silver Parquet I/O |
| Cloud Storage | google-cloud-storage | ^3.10 | Bronze JSONL + Silver Parquet |
| Data Warehouse | google-cloud-bigquery | ^3.41 | Gold `mip_gold.products` |
| Cache | redis-py | ^7.4 | Schema fingerprint → YAML cache |
| Vector DB | ChromaDB | >=0.6 | Observability audit corpus for RAG chatbot |
| Streaming | kafka-python-ng | ^2.2 | Producer / consumer for Bronze ingest |
| Orchestration | Apache Airflow | docker-compose | 9 DAGs |
| Metrics | prometheus-client | ^0.25 | Pushgateway on `:9091` |
| Dashboarding | Grafana | docker | Pipeline metrics + anomaly panels |
| Relational Store | PostgreSQL | docker | Observability audit + anomaly tables |
| Anomaly ML | sklearn | ^0.23 | Isolation Forest on run metrics |
| HITL UI | Streamlit | ^1.56 | 7-step wizard + observability chatbot |
| API Server | FastAPI (uvicorn) | — | MCP observability server on `:8001` |
| ML Tracking | MLflow | docker | Experiment tracking for enrichment tuning |
| Structured Logs | structlog | ^24 | Machine-parseable run logs |
| Testing | pytest | ^8 | Unit + integration |

---

## 4. 🚀 Phase Progression

### Phase 1 — Canonical Domain Schemas & Registry
Defined per-domain canonical schemas under `config/schemas/<domain>_schema.json` (nutrition, safety, pricing, plus experimental retail, finance, manufacturing). The block registry at `src/registry/block_registry.py` knows the default sequence for each domain and expands sentinels (`__generated__`, `dedup_stage`, `enrich_stage`) at runtime. The canonical 14-column unified schema fell out of laying six source schemas side by side and recognizing that USDA's `description`, OFF's `product_name_en`, openFDA's `product_description`, and ESCI's `product_title` are conceptually the same field.

### Phase 2 — Three-Agent LangGraph Flow
Agents 1–3 analyze the source, critique the schema mapping, and plan the block sequence. Agent 2 (the critic) was made opt-in via `--with-critic` after testing showed it doubled LLM cost while catching ~15% more bad ops — a worthwhile option for pathological schemas but unnecessary overhead for well-behaved sources.

### Phase 3 — YAML-Only Transforms & Chunked Streaming
Replaced the initial prototype's runtime Python codegen (`sandbox.py`) with a fixed declarative YAML action set after discovering that generated code was undiffable, unauditable, and a single bad LLM response could quarantine 80% of rows with no trace. `PipelineRunner.run_chunked()` drives execution at 10K rows/chunk, keeping memory flat across source size.

### Phase 4 — Three-Tier Enrichment Cascade
S1 deterministic rules → S2 FAISS KNN → S3 LLM-RAG. The allergen near-miss during S3 prompt tuning — where an LLM confidently labeled a barley product "gluten-free" — drove the hard safety boundary: S2 and S3 touch only `primary_category`. The post-run assertion tripwire in `LLMEnrichBlock` was added the same day.

### Phase 5 — Medallion Storage, Airflow, and Data Quality Observability
Landed the Bronze/Silver/Gold structure in GCS + BigQuery with nine Airflow DAGs. Every DAG shells out to the CLI rather than importing the graph — isolating Airflow from Python env drift. The observability plane was refactored into a best-effort layer after early integration showed inline Pushgateway exports blocking runs on flaky networks.

---

## 5. 🔄 Pipeline Flows

### Flow 1: Three-Agent Schema Analysis

**Agent 1 — Schema Analyzer** (`analyze_schema` node) receives an adaptively sampled ~5K-row DataFrame and maps source columns to the canonical domain schema target. Emits an ordered operation list of `RENAME`, `CAST`, `FORMAT`, `DELETE`, `ADD`, `SPLIT`, `UNIFY`, `DERIVE` ops. On the first run for a new source, also derives unified column names and appends enrichment and DQ columns to the schema file. Default model: `deepseek/deepseek-chat` via `get_orchestrator_llm()`.

**Agent 2 — Schema Critic** (`critique_schema` node) runs only with `--with-critic` and when no Redis YAML cache hit is present. Validates Agent 1's operation list against seven deterministic rules (no duplicate renames, no `CAST` without a declared type, `SPLIT` arity matches source column pattern, etc.). Rejected ops are dropped; amended ops replace originals.

**Agent 3 — Sequence Planner** (`plan_sequence` node) reorders the block sequence — it can only reorder, never add or remove. Any block the LLM drops is re-appended automatically before `dq_score_post`. Agent 3 also writes the complete cacheable blob (column_mapping, operations, gaps, full YAML text, planned sequence) into Redis under the `yaml` prefix.

---

### Flow 2: YAML-Only Transform Execution

Generated transforms serialize via `src/blocks/mapping_io.py` to:
```
src/blocks/generated/<domain>/DYNAMIC_MAPPING_<source_stem>.yaml
```

On subsequent runs, `_discover_generated_blocks()` loads each YAML as a `DynamicMappingBlock` executing a fixed declarative action set:

| Action | Purpose |
|:---|:---|
| `set_null`, `type_cast`, `rename` | Basic field normalization |
| `coalesce`, `concat_columns` | Multi-source field merging |
| `json_array_extract_multi` | Nested JSON extraction |
| `regex_replace`, `split_on_delimiter` | Pattern-based transforms |
| `derive_from_template` | Computed fields from expressions |

`PipelineRunner.run()` applies `column_mapping` (source → unified names) before iterating the block sequence. Every downstream block reads unified names (`product_name`, `brand_name`, `ingredients`, etc.) regardless of source.

---

### Flow 3: Redis Cache Short-Circuit

`src/cache/client.py` — Redis wrapper with SQLite fallback (`output/cache.db`, WAL-mode) when Redis is unreachable.

| Prefix | TTL | Contents |
|:---|:---|:---|
| `yaml` | 30 days | Complete cacheable mapping — column_mapping, operations, gaps, block_sequence, full YAML text |
| `llm` | 7 days | S3 enrichment LLM responses |
| `emb` | 30 days | sentence-transformers embeddings |
| `dedup` | 14 days | Fuzzy dedup cluster keys |

A `yaml` cache hit sets `cache_yaml_hit` in state → `route_after_analyze_schema` skips Agents 1, 2, and 3. The cache blob is only written in `plan_sequence_node` (Agent 3's node) — contributing a new field from any agent requires extending this single write site.

---

### Flow 4: Three-Tier Enrichment Cascade

`src/blocks/llm_enrich.py` orchestrates three tiers for rows with missing fields:

**S1 Deterministic** (`src/enrichment/deterministic.py`) — regex and keyword extraction for `allergens`, `is_organic`, `dietary_tags`, plus a first pass on `primary_category`.

**S2 FAISS KNN** (`src/enrichment/embedding.py`) — encodes the product's name + description, queries `corpus/faiss_index.bin` (IndexFlatIP over L2-normalized vectors ≡ cosine similarity), takes top-`K_NEIGHBORS=5` and votes on `primary_category` with threshold `VOTE_SIMILARITY_THRESHOLD=0.45` and confidence floor `CONFIDENCE_THRESHOLD_CATEGORY=0.60`.

**S3 LLM-RAG** (`src/enrichment/llm_tier.py`) — default model `groq/llama-3.1-8b-instant`; prompt includes the row plus top-3 S2 neighbors as RAG context, returning a category with a confidence.

**Hard safety rule:** S2 and S3 touch only `primary_category`. Allergens, dietary tags, and `is_organic` are S1-only. `LLMEnrichBlock` has a post-run assertion that warns if any S3-resolved row has a safety field differing from its post-S1 state — that warning is a tripwire, not a recommendation.

Both S2 and S3 push resolved rows back into the FAISS corpus, so later runs get better neighbors and a lower S3 cost cascade.

---

### Flow 5: Pipeline Modes (Full / Silver / Gold)

| Mode | Sequence | Output |
|:---|:---|:---|
| `full` (default) | `dq_score_pre → __generated__ → cleaning → dedup_stage → enrich_stage → dq_score_post` | CSV to `output/` |
| `silver` | Schema transform only — no dedup, no enrichment | Parquet to `gs://mip-silver-2024/<source>/<YYYY/MM/DD>/` + watermark update |
| `gold` | Reads all Silver Parquet for source+date, runs dedup + enrichment + DQ scoring | Appends to BigQuery `mip_gold.products` |

---

### Flow 6: Airflow DAG Chain

All DAGs mount `src/` and `config/` from repo root into the Airflow container and call the CLI / gold pipeline rather than importing the graph directly.

| DAG | Schedule | Action |
|:---|:---|:---|
| `usda_incremental_dag` / `off_incremental_dag` / `openfda_incremental_dag` | 02:00–05:00 UTC | Ingest source → GCS Bronze JSONL |
| `bronze_to_bq_dag` | 03:00–06:00 | Load Bronze JSONL → BigQuery staging |
| `bronze_to_silver_dag` | 07:00 | Watermark-gated: new Bronze partitions → ETL pipeline → Silver Parquet |
| `silver_to_gold_dag` | 09:00 | ExternalTaskSensor waits for bronze_to_silver; Silver → dedup + enrichment → BigQuery |
| `uc2_anomaly_dag` | Hourly | Isolation Forest anomaly detection over Prometheus metrics per source (needs ≥5 runs) |
| `uc2_chunker_dag` | Every 5 min | Postgres `audit_events` → MiniLM embedding → ChromaDB `audit_corpus` for RAG chatbot |
| `esci_dag` / `usda_dag` | Manual | ESCI ingestion / full USDA backfill |

---

### Flow 7: Data Quality Observability Plane

`src/uc2_observability/` — fully independent observability layer that never blocks an ETL run:

| Component | Purpose |
|:---|:---|
| `log_writer.py` | Atomic JSON run logs to `output/run_logs/` after every run |
| `log_store.py` | Read-only query interface over run history |
| `rag_chatbot.py` | Structured retrieval + LLM synthesis; returns `ChatResponse(answer, cited_run_ids, context_run_count)` |
| `metrics_exporter.py` | Pushes 12 labelled Prometheus gauges to Pushgateway; isolated `CollectorRegistry`; never raises on network failure |
| `anomaly_detector.py` | Isolation Forest on Prometheus metrics for last N runs; pushes `etl_anomaly_flag=1`; writes to Postgres `anomaly_reports` |
| `chunker.py` | Reads new `audit_events` from Postgres since last cursor, embeds with all-MiniLM-L6-v2, upserts into ChromaDB |
| `kafka_to_pg.py` | Kafka → Postgres consumer; demuxes `pipeline.events` into `audit_events`, `block_trace`, `quarantine_rows`, `dedup_clusters` |
| `mcp_server.py` | FastAPI on `:8001` with 7 MCP-style tool endpoints (Prometheus, Postgres, Redis) |

---

## 6. 🖥️ CLI & Streamlit Dashboard

### MCP Observability Server Endpoints

FastAPI application at `:8001` exposing 7 MCP-style tool endpoints backed by Prometheus, Postgres, and Redis.

| Method | Endpoint | Description |
|:---|:---|:---|
| GET | `/tools` | Enumerates all available MCP tools with their JSON schemas |
| POST | `/tools/run_history` | Query recent pipeline runs filtered by source, status, date range |
| POST | `/tools/block_trace` | Per-block `rows_in/rows_out/duration_ms` for a given run |
| POST | `/tools/quarantine_sample` | Sample quarantined rows with failure reason |
| POST | `/tools/dedup_clusters` | Fuzzy dedup cluster membership for a source |
| POST | `/tools/anomaly_report` | Last Isolation Forest anomaly report for a source |
| POST | `/tools/enrichment_cost` | S1/S2/S3 call counts and token costs per run |
| POST | `/tools/dq_trend` | DQ score delta trend for a source over N runs |
| GET | `/docs` | Swagger UI |

### Streamlit Dashboard

Entry point: `poetry run streamlit run app.py` → `http://localhost:8501`

| Mode | What It Shows |
|:---|:---|
| **Pipeline** | 7-node wizard with HITL gates; executes one node at a time via `run_step()`. Schema analysis → YAML preview → block execution → DQ scoring → save output. |
| **Observability** | Multi-turn chatbot over run logs with refresh button and cited run ID expanders. Grafana dashboard screenshot card with link to live snapshot. |

---

## 7. ⚙️ Setup & Installation

### Prerequisites

- Python 3.11+ and Poetry installed
- Docker and Docker Compose installed
- A GCP project with GCS buckets (`mip-bronze-2024`, `mip-silver-2024`) and BigQuery access (`mip-platform-2024`)
- API keys: `DEEPSEEK_API_KEY`, `GROQ_API_KEY` (minimum); `ANTHROPIC_API_KEY` optional for critic
- Redis (provided by `docker-compose.yml`; SQLite fallback if absent)

### Step 1: Clone the Repository

```bash
git clone https://github.com/BigDataIA-Spring26-MIP/Marketplace-Intelligence-Platform.git
cd Marketplace-Intelligence-Platform
```

### Step 2: Configure Environment Variables

```bash
cp .env.example .env
```

Configure the following variable groups inside `.env`:

| Variable Group | Keys |
|:---|:---|
| LLM Providers | `DEEPSEEK_API_KEY`, `GROQ_API_KEY`, `ANTHROPIC_API_KEY` (optional) |
| Model Overrides | `ORCHESTRATOR_LLM`, `CRITIC_LLM`, `ENRICHMENT_LLM`, `OBSERVABILITY_LLM` |
| GCP | `GOOGLE_APPLICATION_CREDENTIALS` — path to GCP service account JSON |
| GCS Buckets | `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET` |
| Redis | `REDIS_URL` — defaults to `redis://localhost:6379`; SQLite fallback used if unreachable |
| Corpus | `FAISS_INDEX_PATH`, `METADATA_PATH` — persistent corpus for S2 KNN |
| Observability | `UC2_PUSHGATEWAY_URL`, `UC2_KAFKA_BOOTSTRAP`, `UC2_PG_DSN` |

### Step 3: Install Dependencies

```bash
poetry install
```

### Step 4: Bring Up the Platform Stack

```bash
docker-compose -p mip up -d
docker ps
```

Starts Kafka, Airflow, Postgres, Prometheus, Pushgateway, Grafana, ChromaDB, Redis, and MLflow. Service URLs are in [`ENDPOINTS.md`](ENDPOINTS.md).

### Step 5: (One-Time) Build the FAISS Enrichment Corpus

```bash
poetry run python scripts/build_corpus.py
# Or limit for a smoke test:
poetry run python scripts/build_corpus.py --limit 10000
```

Seeds `corpus/faiss_index.bin` (gitignored) and `corpus/corpus_metadata.json` from USDA FoodData Central.

### Step 6: Run the Demo

```bash
poetry run python demo.py
# Variants:
poetry run python demo.py --no-cache        # bypass Redis
poetry run python demo.py --flush-cache     # clear pipeline cache keys first
```

Runs three pipeline passes (USDA → FDA → FDA replay) and prints cache-hit telemetry showing Agents 1/2/3 being short-circuited on the replay.

### Step 7: Run the Primary CLI

```bash
poetry run python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
poetry run python -m src.pipeline.cli --source data/fda_recalls_sample.csv --domain safety --resume
poetry run python -m src.pipeline.cli --source data/usda_sample_raw.csv --force-fresh
poetry run python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/*.jsonl --mode silver
poetry run python -m src.pipeline.cli --source ... --with-critic   # enable Agent 2
```

### Step 8: Gold Layer (BigQuery)

```bash
poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21
```

### Step 9: Launch the Streamlit Wizard

```bash
poetry run streamlit run app.py
# http://localhost:8501
```

### Step 10: Launch the MCP Observability Server

```bash
uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001
# http://localhost:8001/docs
```

### Step 11: Run Tests

```bash
poetry run pytest
poetry run pytest -m "not integration"        # skip GCS-dependent tests
poetry run pytest tests/unit/test_cache_client.py
poetry run pytest tests/integration/test_cache_pipeline.py
```

---

## 8. 📊 Summary & Key Takeaways

### LLM Provider Strategy

| Provider | Tasks | Default Model |
|:---|:---|:---|
| DeepSeek | Orchestrator (Agents 1, 3) | `deepseek/deepseek-chat` |
| Groq | Enrichment (S3), Observability chatbot | `groq/llama-3.1-8b-instant` |
| Anthropic (optional) | Agent 2 critic | `anthropic/claude-sonnet-4-6` |

All LLM calls route through LiteLLM. Swapping the entire platform to a different provider is a single env-var change in `src/models/llm.py`.

### Evaluation Metrics

| Dimension | Metric | Target |
|:---|:---|:---|
| Onboarding cost | Lines of Python hand-written per new source | 0 lines |
| Schema conformance | % of rows landing in Silver matching canonical column set | 100% |
| Cache replay efficiency | % of runs skipping all 3 agents via YAML cache hit | >80% steady-state |
| Data quality delta | `dq_score_post − dq_score_pre` per run | Positive delta, trending up |
| Quarantine rate | % of rows quarantined per run | <2% on clean sources; <8% on OFF |
| Enrichment cost cascade | S1/S2/S3 distribution per run | S1 > 60%, S3 < 15% |
| Safety invariant | S3-resolved rows with safety field differing from post-S1 state | 0 — tripwire |
| Pipeline latency | Wall-clock seconds per 100K rows | <90s cache hit, <300s cold |

---

## 9. 🧠 Design Decisions & Tradeoffs

**YAML-only transforms, no runtime Python codegen.** Generated mappings live as diffable YAML under `src/blocks/generated/<domain>/`. This locks the action surface to a fixed declarative set — a deliberate constraint over flexibility. After six source datasets onboarded, we never needed something the action set couldn't express.

**Task-based LLM routing via LiteLLM.** Model selection is determined by task, not call site. Swapping the whole platform to a different provider is an env-var change. The trade-off is that every new feature needs its task type registered in `src/models/llm.py` before it can use the LLM.

**Agent 2 off by default.** Single-agent runs are materially cheaper and fast enough for well-behaved sources. The critic is gated behind `--with-critic` for pathological schemas. Cache hits skip it unconditionally.

**Agent 3 cannot add or remove blocks.** The sequence planner is deliberately constrained — any block the LLM drops is re-appended automatically before `dq_score_post`. The LLM can shuffle; it cannot delete. This made the pipeline shape invariant across Agent 3 non-determinism.

**Hard safety boundary in enrichment.** S2 and S3 touch only `primary_category`. Allergens, dietary tags, and `is_organic` are S1-only. A false positive on a safety field is categorically worse than a null, and there is no circumstance under which we accept an LLM-inferred allergen claim.

**Redis cache short-circuits all three agents.** A complete YAML cache hit makes replay runs nearly free. The trade-off is cache coherence: `plan_sequence_node` is the single write site, and any new field produced by an agent must be added there or replayed runs will silently drop it.

**Chunked streaming as default, not optional.** `run_pipeline_node` always calls `run_chunked()` at 10K rows/chunk. Any block that needs cross-chunk state (like fuzzy dedup) maintains its own accumulator and flushes at end-of-stream.

**Observability is best-effort.** Every observability emit is wrapped in `try/except`. An outage — Pushgateway down, Postgres unreachable, ChromaDB lagged — never blocks an ETL run. During an outage we accept silent gaps in the audit trail over pipeline failure.

**Airflow calls the CLI, not the graph.** DAGs shell out to `python -m src.pipeline.cli` rather than importing `src.agents.graph`. This isolates Airflow from Python env drift and lets DAG-level retries restart the CLI process cleanly.

---

## 10. ⚠️ Known Limitations

1. **Hybrid Search and Recommendations modules are scaffolding only.** `src/uc3_search/` (hybrid search) and `src/uc4_recommendations/` (association-rule recommendations) contain placeholder classes. They are not wired into `demo.py`, `app.py`, the graph, or the CLI.

2. **LLM non-determinism in Agent 1.** Two identical runs against the same source with Redis disabled can produce slightly different operation lists. First-run mappings should always be treated as a draft for human review via the HITL gate.

3. **Cache-fingerprint coherence risk.** `plan_sequence_node` is the only write site for the complete cacheable blob. If new fields are added to what Agents 1 or 2 produce without extending the write site, cache hits will silently replay stale state.

4. **ChromaDB is single-node.** At a few hundred runs per day it is fine. For 10x scale-out a managed vector store or sharded Chroma cluster would be needed.

5. **Anomaly detection needs ≥5 prior runs per source.** Brand-new sources get no anomaly signal for their first few runs. Acceptable because new-source onboarding is always human-gated via the HITL wizard.

6. **S1 deterministic rules are English-centric.** Allergen and dietary-tag regex patterns are tuned for English product descriptions. OFF's multilingual catalog gets partial coverage; non-English allergen terms fall through to S1's null branch by design.

7. **No streaming anomaly replay.** When a run is flagged as anomalous, an analyst must manually re-run it after investigation. There is no auto-retry path or circuit breaker.

---

## 👥 Team Members

| Member | Contributions |
|:---|:---|
| **Bhavya** | Three-agent LangGraph flow (Agents 1–3), YAML mapping I/O layer, Redis cache short-circuit with schema fingerprint, SQLite fallback, full cacheable blob write in `plan_sequence_node`, chunked streaming runner with `__generated__` sentinel expansion |
| **Aqeel** | Data Quality Observability plane end-to-end — `RunLogWriter`, `MetricsExporter`, `AnomalyDetector`, `chunker.py`, `kafka_to_pg.py`, FastAPI MCP server; three-tier enrichment cascade design with safety boundary; Airflow DAG chain |
| **Deepika** | Schema analyzer, domain schema registration, bootstrap path for CLI, enrichment column extensions (`allergens`, `primary_category`, `dietary_tags`, `is_organic`, DQ columns), per-domain schema JSON on first run, project documentation and Codelabs |

---

*DAMG 7245: Big Data and Intelligent Analytics — Spring 2026 | Northeastern University | Group 5*
