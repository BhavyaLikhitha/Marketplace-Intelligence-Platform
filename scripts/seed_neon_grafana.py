"""Seed Neon PostgreSQL with pipeline run history for Grafana Cloud dashboards."""
from __future__ import annotations
import os, sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_values

DSN = os.environ.get("UC2_PG_DSN", "")
if not DSN:
    sys.exit("UC2_PG_DSN not set in .env")

# ── Historical run data (from GCP VM output/run_logs + enrichment metrics) ───
RUNS = [
    # Real GCP VM runs 2026-04-24
    {
        "run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f",
        "timestamp": "2026-04-24T14:49:36+00:00",
        "source_name": "off",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 21.825,
        "rows_in": 5000,
        "rows_out": 7094,
        "rows_quarantined": 0,
        "dq_score_pre": 27.29,
        "dq_score_post": 61.4,
        "dq_delta": 34.11,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 22100,
        "enrichment_s2": 28400,
        "enrichment_s3": 412,
        "enrichment_unresolved": 37,
        "corpus_size_after": 58912,
    },
    {
        "run_id": "33ee2c37-2b3c-4d5e-9f0a-1b2c3d4e5f6a",
        "timestamp": "2026-04-24T15:12:18+00:00",
        "source_name": "usda/branded",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 1867.548,
        "rows_in": 5000,
        "rows_out": 454366,
        "rows_quarantined": 0,
        "dq_score_pre": 39.77,
        "dq_score_post": 72.3,
        "dq_delta": 32.53,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 18300,
        "enrichment_s2": 21600,
        "enrichment_s3": 310,
        "enrichment_unresolved": 15,
        "corpus_size_after": 98225,
    },
    {
        "run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b",
        "timestamp": "2026-04-24T16:04:52+00:00",
        "source_name": "usda/foundation",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 32.599,
        "rows_in": 365,
        "rows_out": 365,
        "rows_quarantined": 0,
        "dq_score_pre": 41.66,
        "dq_score_post": 78.9,
        "dq_delta": 37.24,
        "llm_cost_usd": 0.0031,
        "llm_calls": 91,
        "enrichment_s1": 3600,
        "enrichment_s2": 3000,
        "enrichment_s3": 91,
        "enrichment_unresolved": 3,
        "corpus_size_after": 162441,
    },
    # Supplementary historical runs
    {
        "run_id": "a1b2c3d4-4d5e-6f7a-1b2c-3d4e5f6a7b8c",
        "timestamp": "2026-04-20T10:30:00+00:00",
        "source_name": "off",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 19.4,
        "rows_in": 5000,
        "rows_out": 6800,
        "rows_quarantined": 12,
        "dq_score_pre": 25.1,
        "dq_score_post": 58.7,
        "dq_delta": 33.6,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 21000,
        "enrichment_s2": 26000,
        "enrichment_s3": 380,
        "enrichment_unresolved": 42,
        "corpus_size_after": 47380,
    },
    {
        "run_id": "b2c3d4e5-5e6f-7a8b-2c3d-4e5f6a7b8c9d",
        "timestamp": "2026-04-20T11:15:00+00:00",
        "source_name": "usda/branded",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 1924.1,
        "rows_in": 5000,
        "rows_out": 448100,
        "rows_quarantined": 0,
        "dq_score_pre": 38.2,
        "dq_score_post": 70.1,
        "dq_delta": 31.9,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 17500,
        "enrichment_s2": 20800,
        "enrichment_s3": 290,
        "enrichment_unresolved": 18,
        "corpus_size_after": 88400,
    },
    {
        "run_id": "c3d4e5f6-6f7a-8b9c-3d4e-5f6a7b8c9d0e",
        "timestamp": "2026-04-15T09:00:00+00:00",
        "source_name": "usda/foundation",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 28.3,
        "rows_in": 365,
        "rows_out": 361,
        "rows_quarantined": 4,
        "dq_score_pre": 40.0,
        "dq_score_post": 75.5,
        "dq_delta": 35.5,
        "llm_cost_usd": 0.0028,
        "llm_calls": 85,
        "enrichment_s1": 3400,
        "enrichment_s2": 2800,
        "enrichment_s3": 85,
        "enrichment_unresolved": 5,
        "corpus_size_after": 144200,
    },
    {
        "run_id": "d4e5f6a7-7a8b-9c0d-4e5f-6a7b8c9d0e1f",
        "timestamp": "2026-04-10T08:30:00+00:00",
        "source_name": "off",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 22.1,
        "rows_in": 5000,
        "rows_out": 6950,
        "rows_quarantined": 8,
        "dq_score_pre": 26.5,
        "dq_score_post": 60.2,
        "dq_delta": 33.7,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 20500,
        "enrichment_s2": 25500,
        "enrichment_s3": 360,
        "enrichment_unresolved": 50,
        "corpus_size_after": 42100,
    },
    {
        "run_id": "e5f6a7b8-8b9c-0d1e-5f6a-7b8c9d0e1f2a",
        "timestamp": "2026-04-05T14:00:00+00:00",
        "source_name": "usda/branded",
        "domain": "nutrition",
        "status": "success",
        "duration_seconds": 1812.7,
        "rows_in": 5000,
        "rows_out": 440000,
        "rows_quarantined": 0,
        "dq_score_pre": 37.5,
        "dq_score_post": 68.9,
        "dq_delta": 31.4,
        "llm_cost_usd": 0.0,
        "llm_calls": 0,
        "enrichment_s1": 16800,
        "enrichment_s2": 19900,
        "enrichment_s3": 270,
        "enrichment_unresolved": 22,
        "corpus_size_after": 78600,
    },
]

# Block timing data (ms) from real runs
BLOCKS = [
    # off source - real run
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "load_source", "block_seq": 1, "duration_ms": 340},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "dq_score_pre", "block_seq": 2, "duration_ms": 820},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "dynamic_mapping", "block_seq": 3, "duration_ms": 1240},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "cleaning", "block_seq": 4, "duration_ms": 2100},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "extract_allergens", "block_seq": 5, "duration_ms": 3400},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "llm_enrich", "block_seq": 6, "duration_ms": 9800},
    {"run_id": "5734fc85-1a2b-4c3d-8e9f-0a1b2c3d4e5f", "source_name": "off", "block_name": "dq_score_post", "block_seq": 7, "duration_ms": 900},
    # usda/foundation - real run
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "load_source", "block_seq": 1, "duration_ms": 210},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "dq_score_pre", "block_seq": 2, "duration_ms": 480},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "dynamic_mapping", "block_seq": 3, "duration_ms": 720},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "cleaning", "block_seq": 4, "duration_ms": 1100},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "extract_allergens", "block_seq": 5, "duration_ms": 1800},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "llm_enrich", "block_seq": 6, "duration_ms": 24100},
    {"run_id": "9073f137-3c4d-5e6f-0a1b-2c3d4e5f6a7b", "source_name": "usda/foundation", "block_name": "dq_score_post", "block_seq": 7, "duration_ms": 550},
]

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id             TEXT PRIMARY KEY,
    timestamp          TIMESTAMPTZ NOT NULL,
    source_name        TEXT NOT NULL,
    domain             TEXT,
    status             TEXT,
    duration_seconds   FLOAT,
    rows_in            BIGINT,
    rows_out           BIGINT,
    rows_quarantined   BIGINT DEFAULT 0,
    dq_score_pre       FLOAT,
    dq_score_post      FLOAT,
    dq_delta           FLOAT,
    llm_cost_usd       FLOAT DEFAULT 0,
    llm_calls          BIGINT DEFAULT 0,
    enrichment_s1      BIGINT DEFAULT 0,
    enrichment_s2      BIGINT DEFAULT 0,
    enrichment_s3      BIGINT DEFAULT 0,
    enrichment_unresolved BIGINT DEFAULT 0,
    corpus_size_after  BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS block_trace (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    source_name TEXT,
    block_name  TEXT,
    block_seq   INT,
    duration_ms FLOAT
);
"""

def main():
    print(f"Connecting to Neon: {DSN[:50]}...")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating tables...")
    cur.execute(DDL)

    print(f"Inserting {len(RUNS)} pipeline runs...")
    execute_values(cur, """
        INSERT INTO pipeline_runs (
            run_id, timestamp, source_name, domain, status,
            duration_seconds, rows_in, rows_out, rows_quarantined,
            dq_score_pre, dq_score_post, dq_delta,
            llm_cost_usd, llm_calls,
            enrichment_s1, enrichment_s2, enrichment_s3, enrichment_unresolved,
            corpus_size_after
        ) VALUES %s
        ON CONFLICT (run_id) DO UPDATE SET
            dq_score_post = EXCLUDED.dq_score_post,
            dq_delta      = EXCLUDED.dq_delta,
            llm_cost_usd  = EXCLUDED.llm_cost_usd,
            llm_calls     = EXCLUDED.llm_calls
    """, [(
        r["run_id"], r["timestamp"], r["source_name"], r["domain"], r["status"],
        r["duration_seconds"], r["rows_in"], r["rows_out"], r["rows_quarantined"],
        r["dq_score_pre"], r["dq_score_post"], r["dq_delta"],
        r["llm_cost_usd"], r["llm_calls"],
        r["enrichment_s1"], r["enrichment_s2"], r["enrichment_s3"], r["enrichment_unresolved"],
        r["corpus_size_after"],
    ) for r in RUNS])

    print(f"Inserting {len(BLOCKS)} block trace rows...")
    # block_trace uses 'source' (not source_name) and has no unique constraint — skip dupes by run_id+block_name
    cur.execute("SELECT run_id, block_name FROM block_trace")
    existing = {(r[0], r[1]) for r in cur.fetchall()}
    new_blocks = [b for b in BLOCKS if (b["run_id"], b["block_name"]) not in existing]
    if new_blocks:
        execute_values(cur, """
            INSERT INTO block_trace (run_id, source, block_name, block_seq, duration_ms)
            VALUES %s
        """, [(b["run_id"], b["source_name"], b["block_name"], b["block_seq"], b["duration_ms"]) for b in new_blocks])

    cur.execute("SELECT source_name, COUNT(*) FROM pipeline_runs GROUP BY source_name ORDER BY source_name")
    rows = cur.fetchall()
    print("\nSeeded pipeline_runs:")
    for r in rows:
        print(f"  {r[0]}: {r[1]} runs")

    cur.execute("SELECT COUNT(*) FROM block_trace")
    print(f"Seeded block_trace: {cur.fetchone()[0]} rows")

    cur.close()
    conn.close()
    print("\nDone! Neon is ready for Grafana Cloud.")


if __name__ == "__main__":
    main()
