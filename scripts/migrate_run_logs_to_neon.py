"""One-time migration: copy run log JSON files from disk into Neon run_logs table."""
import json
import glob
import psycopg2
from pathlib import Path

DSN = "postgresql://neondb_owner:npg_m8PE6UajfuCi@ep-proud-frost-anssc33j-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require"
LOG_DIR = Path.home() / "Marketplace-Intelligence-Platform" / "output" / "run_logs"

files = sorted(glob.glob(str(LOG_DIR / "*.json")))
print(f"Found {len(files)} log files in {LOG_DIR}")

conn = psycopg2.connect(DSN)
cur = conn.cursor()
inserted = 0
skipped = 0

for f in files:
    try:
        d = json.loads(Path(f).read_text())
        cur.execute(
            """
            INSERT INTO run_logs (run_id, source_name, domain, status, timestamp, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                d.get("run_id"),
                d.get("source_name"),
                d.get("domain"),
                d.get("status"),
                d.get("timestamp"),
                json.dumps(d),
            ),
        )
        inserted += 1
    except Exception as e:
        print(f"  skip {f}: {e}")
        skipped += 1

conn.commit()
cur.close()
conn.close()
print(f"Done — migrated {inserted} run logs to Neon  ({skipped} skipped)")
