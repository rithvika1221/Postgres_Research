#!/usr/bin/env python3
"""
Transaction Size Experiment - Load Generator

Tests how transaction size (rows per commit) affects replication lag.
TPS = COMMITS per second (not rows). Phase 6 = 5000 txns/sec × 1000 rows = 5M rows/sec.

Schedule (2 min each, 12 min total):
  Phase 1: 1 user, 100 TPS, 1 row/txn
  Phase 2: 4 users, 500 TPS, 10 rows/txn
  Phase 3: 8 users, 1000 TPS, 50 rows/txn
  Phase 4: 16 users, 2500 TPS, 100 rows/txn
  Phase 5: 32 users, 5000 TPS, 500 rows/txn
  Phase 6: 64 users, 5000 TPS, 1000 rows/txn

Each worker receives ALL config as direct arguments (no shared queue).
Workers are spawned fresh each phase.

Run on: PUBLISHER VM
Requires: psycopg2
Usage:   python load_generator.py
"""

import os
import sys
import time
import random
import signal
import argparse
from datetime import datetime, timedelta
from multiprocessing import Process, Value, Event

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pub",
    "user": "postgres",
    "password": "Aarush@123",
}

# Schedule: (phase_name, num_workers, target_tps, rows_per_commit, duration_seconds)
SCHEDULE = [
    ("Phase 1: 1 user, 100 TPS, 1 row/txn",       1,   100,    1, 120),
    ("Phase 2: 4 users, 500 TPS, 10 rows/txn",     4,   500,   10, 120),
    ("Phase 3: 8 users, 1000 TPS, 50 rows/txn",    8,  1000,   50, 120),
    ("Phase 4: 16 users, 2500 TPS, 100 rows/txn",  16,  2500,  100, 120),
    ("Phase 5: 32 users, 5000 TPS, 500 rows/txn",  32,  5000,  500, 120),
    ("Phase 6: 64 users, 5000 TPS, 1000 rows/txn", 64,  5000, 1000, 120),
]

STATUS_FILE = "tps_phase.txt"
POOL_SIZE = 200  # Pre-generated payload pool size


def generate_payload(size_bytes):
    """Fast payload generation using os.urandom."""
    return os.urandom((size_bytes + 1) // 2).hex()[:size_bytes]


def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def write_status(phase_name, target_tps, num_workers, rows_per_commit):
    """Write current phase info for monitors."""
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(f"{phase_name}\n")
            f.write(f"target_tps={target_tps}\n")
            f.write(f"num_workers={num_workers}\n")
            f.write(f"rows_per_commit={rows_per_commit}\n")
            f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    except IOError as e:
        print(f"  WARNING: Cannot write status file: {e}")


def worker(worker_id, num_workers, target_tps, rows_per_commit, stop_event, total_inserts):
    """
    Worker process. Receives ALL config as direct arguments.
    Pre-generates payload pool, then does rate-limited batch INSERTs.
    TPS here means COMMITS per second, each with rows_per_commit rows.
    """
    random.seed(worker_id + int(time.time()))
    conn = None
    local_count = 0
    worker_tps = target_tps / num_workers  # commits per second for this worker

    try:
        conn = get_connection()
        cur = conn.cursor()
        print(f"  [Worker {worker_id:>2}] Connected. TPS={worker_tps:.1f}, rows/commit={rows_per_commit}")

        # Pre-generate payload pool ONCE (fast!)
        print(f"  [Worker {worker_id:>2}] Pre-generating {POOL_SIZE} payload pairs...")
        pool_100 = [generate_payload(100) for _ in range(POOL_SIZE)]
        pool_1000 = [generate_payload(1000) for _ in range(POOL_SIZE)]
        print(f"  [Worker {worker_id:>2}] Ready. Inserting...")

        pool_idx = 0
        tokens = 0.0
        last_token_time = time.time()

        while not stop_event.is_set():
            now = time.time()
            elapsed = now - last_token_time
            tokens += elapsed * worker_tps
            last_token_time = now
            tokens = min(tokens, worker_tps * 2)

            if tokens >= 1.0:
                # Build batch INSERT with rows_per_commit rows
                batch_values = []
                for _ in range(rows_per_commit):
                    idx = pool_idx % POOL_SIZE
                    batch_values.append((pool_100[idx], pool_1000[idx]))
                    pool_idx += 1

                args_str = ",".join(
                    cur.mogrify("(%s, %s)", v).decode("utf-8") for v in batch_values
                )
                cur.execute(
                    f"INSERT INTO ingest_data (payload, big_payload) VALUES {args_str}"
                )
                conn.commit()
                tokens -= 1.0
                local_count += rows_per_commit
            else:
                if worker_tps > 0:
                    sleep_needed = (1.0 - tokens) / worker_tps
                    time.sleep(min(sleep_needed, 0.05))
                else:
                    time.sleep(0.1)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"  [Worker {worker_id:>2}] ERROR: {e}")
    finally:
        if conn:
            try:
                conn.commit()
                conn.close()
            except Exception:
                pass
        with total_inserts.get_lock():
            total_inserts.value += local_count
        print(f"  [Worker {worker_id:>2}] Done. Inserted {local_count:,} rows ({local_count // max(rows_per_commit, 1):,} commits).")


def main():
    parser = argparse.ArgumentParser(description="Transaction size load generator")
    parser.add_argument("--host", type=str, default=DB_CONFIG["host"])
    parser.add_argument("--port", type=int, default=DB_CONFIG["port"])
    parser.add_argument("--output-dir", default=".", help="Output directory for status file")
    args = parser.parse_args()

    DB_CONFIG["host"] = args.host
    DB_CONFIG["port"] = args.port
    total_duration = sum(p[4] for p in SCHEDULE)

    print("=" * 70)
    print("  PostgreSQL Logical Replication - Load Generator")
    print("  Experiment: Transaction Size (Rows per Commit)")
    print("=" * 70)
    print(f"  Host:       {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Database:   {DB_CONFIG['dbname']}")
    print(f"  Total time: {total_duration}s ({total_duration // 60} min)")
    print(f"\n  Phase Schedule:")
    cumulative = 0
    for name, w, tps, rpc, dur in SCHEDULE:
        m1, s1 = divmod(cumulative, 60)
        m2, s2 = divmod(cumulative + dur, 60)
        print(f"    {m1}:{s1:02d}-{m2}:{s2:02d}  {w:>2} workers | {tps:>5,} TPS | {rpc:>4} rows/commit | = {tps*rpc:>9,} rows/sec")
        cumulative += dur
    print("=" * 70)

    # Verify connection
    print("\n[1/2] Verifying database...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM ingest_data")
        initial_count = cur.fetchone()[0]
        cur.execute("SELECT pubname FROM pg_publication WHERE pubname = 'mypub'")
        pub = cur.fetchone()
        print(f"  Connected. Rows: {initial_count:,}. Publication: {'OK' if pub else 'WARNING: not found!'}")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # Signal handling
    master_stop = Event()

    def signal_handler(signum=None, frame=None):
        print("\n\n  Ctrl+C received, stopping...")
        master_stop.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"[2/2] Starting experiment at {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Expected end: {(datetime.now() + timedelta(seconds=total_duration)).strftime('%H:%M:%S')}\n")

    experiment_start = time.time()
    total_inserts = Value("i", 0)

    try:
        for phase_idx, (phase_name, num_workers, target_tps, rows_per_commit, duration) in enumerate(SCHEDULE):
            if master_stop.is_set():
                break

            print(f"\n  {'='*60}")
            print(f"  {phase_name}")
            print(f"  {num_workers} workers | {target_tps:,} TPS | {rows_per_commit} rows/commit | {duration}s")
            print(f"  {'='*60}")

            write_status(phase_name, target_tps, num_workers, rows_per_commit)

            # Each phase gets a fresh stop event
            phase_stop = Event()
            processes = []

            # Spawn workers with ALL config as direct arguments
            for wid in range(num_workers):
                p = Process(
                    target=worker,
                    args=(wid, num_workers, target_tps, rows_per_commit, phase_stop, total_inserts)
                )
                p.start()
                processes.append(p)

            # Wait for phase duration
            phase_start = time.time()
            phase_end = phase_start + duration

            while time.time() < phase_end and not master_stop.is_set():
                elapsed_phase = int(time.time() - phase_start)
                elapsed_total = int(time.time() - experiment_start)
                remaining = max(0, int(phase_end - time.time()))
                with total_inserts.get_lock():
                    current_total = total_inserts.value
                actual_tps = current_total / elapsed_total if elapsed_total > 0 else 0

                if elapsed_phase % 10 == 0:
                    print(
                        f"    [{elapsed_phase:>3}s/{duration}s] "
                        f"Rows: {current_total:>12,} | "
                        f"Avg rows/s: {actual_tps:>9,.0f} | "
                        f"Target: {target_tps:>5,} commits/s × {rows_per_commit} | "
                        f"Left: {remaining}s"
                    )
                write_status(phase_name, target_tps, num_workers, rows_per_commit)
                time.sleep(5)

            # Stop this phase's workers
            print(f"  Phase {phase_idx + 1} complete. Stopping {num_workers} workers...")
            phase_stop.set()

            deadline = time.time() + 3
            for p in processes:
                remaining = max(0.1, deadline - time.time())
                p.join(timeout=remaining)
            for p in processes:
                if p.is_alive():
                    p.terminate()
            time.sleep(0.3)
            for p in processes:
                if p.is_alive():
                    p.kill()

    except KeyboardInterrupt:
        print("\n\n  Interrupted!")
        master_stop.set()

    finally:
        write_status("COMPLETED", 0, 0, 0)
        total_elapsed = time.time() - experiment_start
        with total_inserts.get_lock():
            final_total = total_inserts.value

        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM ingest_data")
            final_count = cur.fetchone()[0]
            conn.commit()
            conn.close()
            new_rows = final_count - initial_count
        except Exception:
            final_count = "N/A"
            new_rows = final_total

        print(f"\n{'='*70}")
        print("  EXPERIMENT COMPLETE")
        print(f"{'='*70}")
        print(f"  Duration:    {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print(f"  Inserts:     {final_total:,}")
        print(f"  Table rows:  {final_count}")
        print(f"  New rows:    {new_rows:,}")
        if total_elapsed > 0:
            print(f"  Avg rows/s:  {final_total / total_elapsed:,.0f}")
        print(f"{'='*70}")

        try:
            os.remove(STATUS_FILE)
        except Exception:
            pass


if __name__ == "__main__":
    main()
