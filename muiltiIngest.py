import os
import time
import math
import psycopg2
from multiprocessing import Process
from psycopg2.extras import execute_values

HOST = "pg-source2-westus2.postgres.database.azure.com"
DBNAME = "pub"
USER = "postgres"
PASSWORD = os.getenv("PGPASSWORD")  # best practice: set env var instead of hardcoding
PORT = 5432

TOTAL_RECORDS = 10_000_000
WORKERS = 16              # start with 2–4; too many can slow Azure
BATCH_SIZE = 20_000       # rows per INSERT statement per worker (tune: 5k–50k)
SLEEP_SECONDS = 0         # optional pause between batches

BIG_TEXT = "X" * 1000     # 1 KB per row

INSERT_SQL = """
INSERT INTO ingest_data (payload, big_payload)
VALUES %s
"""

def worker_insert(worker_id: int, start_id: int, end_id: int):
    conn = psycopg2.connect(
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        sslmode="require",  # Azure typically requires SSL
    )
    conn.autocommit = False
    cur = conn.cursor()

    print(f"[Worker {worker_id}] Inserting range {start_id}..{end_id - 1}")

    inserted = 0
    t0 = time.time()

    for batch_start in range(start_id, end_id, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, end_id)

        # Build rows for this batch
        rows = [(f"data_{i}", BIG_TEXT) for i in range(batch_start, batch_end)]

        # Fast multi-row insert
        execute_values(
            cur,
            INSERT_SQL,
            rows,
            page_size=len(rows)  # one big VALUES list per batch
        )

        conn.commit()
        inserted += (batch_end - batch_start)

        if inserted % (BATCH_SIZE * 5) == 0:
            elapsed = time.time() - t0
            rate = inserted / elapsed if elapsed > 0 else 0
            print(f"[Worker {worker_id}] Inserted {inserted} rows | ~{rate:,.0f} rows/sec")

        if SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)

    cur.close()
    conn.close()
    print(f"[Worker {worker_id}] Done.")

def main():
    if PASSWORD is None:
        raise RuntimeError("Set PGPASSWORD env var: export PGPASSWORD='your_password'")

    # Split TOTAL_RECORDS across workers
    chunk = math.ceil(TOTAL_RECORDS / WORKERS)
    procs = []

    for w in range(WORKERS):
        start_id = w * chunk
        end_id = min((w + 1) * chunk, TOTAL_RECORDS)
        if start_id >= end_id:
            break

        p = Process(target=worker_insert, args=(w, start_id, end_id))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print("All workers finished.")

if __name__ == "__main__":
    main()
