"""Service health checks for the DataForge topbar."""
from __future__ import annotations
import os
import socket
import logging

import requests

from .redis_cache import cached_query

logger = logging.getLogger(__name__)

REDIS_HOST   = os.getenv("REDIS_HOST",        "localhost")
PG_HOST      = os.getenv("PG_HOST",           "localhost")
PG_PORT      = int(os.getenv("PG_PORT",       "5432"))
KAFKA_HOST   = os.getenv("KAFKA_HOST",        "localhost")
KAFKA_PORT   = int(os.getenv("KAFKA_PORT",    "9092"))
CHROMA_URL   = os.getenv("CHROMA_URL",        "http://localhost:8000")
MLFLOW_URL   = os.getenv("MLFLOW_TRACKING_URI","http://localhost:5000")
GRAFANA_URL  = os.getenv("GRAFANA_BASE_URL",  "http://localhost:3000")


def _tcp_ok(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _http_ok(url: str, timeout: float = 2.0) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False


def _check_services() -> dict[str, str]:
    import os
    results: dict[str, str] = {}

    # Redis — use REDIS_URL if set (cloud), otherwise check host:port
    redis_url = os.getenv("REDIS_URL", "")
    if redis_url:
        try:
            import redis as _redis
            r = _redis.from_url(redis_url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            results["Redis"] = "ok"
        except Exception:
            results["Redis"] = "error"
    else:
        results["Redis"] = "ok" if _tcp_ok(REDIS_HOST, 6379) else "error"

    # Postgres — use UC2_PG_DSN if set (Neon), otherwise check host:port
    pg_dsn = os.getenv("UC2_PG_DSN", "")
    if pg_dsn:
        try:
            import psycopg2
            conn = psycopg2.connect(pg_dsn, connect_timeout=3)
            conn.close()
            results["Postgres"] = "ok"
        except Exception:
            results["Postgres"] = "error"
    else:
        results["Postgres"] = "ok" if _tcp_ok(PG_HOST, PG_PORT) else "error"

    # Kafka, ChromaDB, MLflow — not critical, show warn without blocking
    results["Kafka"]   = "warn"
    results["ChromaDB"] = "warn"
    results["MLflow"]  = "warn"

    # Grafana — only check if URL is explicitly set
    if GRAFANA_URL and GRAFANA_URL != "http://localhost:3000":
        results["Grafana"] = "ok" if _http_ok(f"{GRAFANA_URL}/api/health") else "warn"
    else:
        results["Grafana"] = "warn"

    return results


def check_all_services() -> dict[str, str]:
    return cached_query("ui:health:all", _check_services, ttl=15)


def count_active_runs() -> int:
    try:
        from src.uc2_observability.log_store import RunLogStore
        from datetime import datetime, timezone, timedelta
        store = RunLogStore()
        logs = store.load_all()
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        running = [r for r in logs if r.get("status") == "running"
                   and r.get("timestamp", "") >= cutoff]
        return len(running)
    except Exception:
        return 0
