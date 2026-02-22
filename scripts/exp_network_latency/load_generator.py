#!/usr/bin/env python3
"""
Load Generator — Network Latency Experiment

Tests how network latency affects replication lag at different TPS levels.
The --latency parameter labels the run (e.g., "0ms", "10ms", "50ms").
Actual latency is introduced externally via clumsy or tc netem.

TPS Schedule (2 min each, 10 min total):
  Phase 1: 100 TPS
  Phase 2: 500 TPS
  Phase 3: 1000 TPS
  Phase 4: 2500 TPS
  Phase 5: 5000 TPS

Each worker receives ALL config as direct arguments (no shared queue).
Workers are spawned fresh each phase.

Run on: PUBLISHER VM
Requires: psycopg2
Usage:   python load_generator.py --latency 50ms --workers 16
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

NUM_WORKERS = 16
POOL_SIZE = 200  # Pre-generated payload pool size

TPS_SCHEDULE = [
    ("Phase 1: 100 TPS",   100,  120),
    ("Phase 2: 500 TPS",   500,  120),
    ("Phase 3: 1000 TPS", 1000,  120),
    ("Phase 4: 2500 TPS", 2500,  120),
    ("Phase 5: 5000 TPS", 5000,  120),
]

STATUS_FILE = "tps_phase.txt"


def generate_payload(size_bytes):
    """Fast payload generation using os.urandom."""
    return os.urandom((size_bytes + 1) // 2).hex()[:size_bytes]


def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def write_status(phase_name, target_tps, num_workers, latency_label):
    """Write current phase info for monitors."""
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(f"{phase_name}\n")
            f.write(f"target_tps={target_tps}\n")
            f.write(f"num_workers={num_workers}\n")
            f.write(f"network_latency={latency_label}\n")
            f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    except IOError as e:
        print(f"  WARNING: Cannot write status file: {e}")


def calculate_batch_size(worker_tps):
    if worker_tps <= 10:
        return 2
    elif worker_tps <= 30:
        return 5
    elif worker_tps <= 60:
        return 10
    elif worker_tps <= 150:
        return 25
    elif worker_tps <= 300:
        return 50
    else:
        return 100


def worker(worker_id, num_workers, target_tps, stop_event, total_inserts):
    """
    Worker process. Receives ALL config as direct arguments.
    Pre-generates payload pool, then does rate-limited batch INSERTs.
    """
    random.seed(worker_id + int(time.time()))
    conn = None
    local_count = 0
    worker_tps = target_tps / num_workers

    try:
        conn = get_connection()
        cur = conn.cursor()
        print(f"  [Worker {worker_id:>2}] Connected. TPS={worker_tps:.1f}")

        # Pre-generate payload pool ONCE (fast!)
        print(f"  [Worker {worker_id:>2}] Pre-generating {POOL_SIZE} payload pairs...")
        pool_100 = [generate_payload(100) for _ in range(POOL_SIZE)]
        pool_1000 = [generate_payload(1000) for _ in range(POOL_SIZE)]
        print(f"  [Worker {worker_id:>2}] Ready. Inserting...")

        pool_idx = 0
        tokens = 0.0
        last_token_time = time.time()
        batch_size = calculate_batch_size(worker_tps)

        while not stop_event.is_set():
            now = time.time()
            elapsed = now - last_token_time
            tokens += elapsed * worker_tps
            last_token_time = now
            tokens = min(tokens, worker_tps * 2)

            if tokens >= batch_size:
                # Build batch INSERT
                batch_values = []
                for _ in range(batch_size):
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
                tokens -= batch_size
                local_count += batch_size
            else:
                if worker_tps > 0:
                    sleep_needed = (batch_size - tokens) / worker_tps
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
        print(f"  [Worker {worker_id:>2}] Done. Inserted {local_count:,} rows.")


def main():
    parser = argparse.ArgumentParser(description="Network latency experiment load generator")
    parser.add_argument("--host", type=str, default=DB_CONFIG["host"])
    parser.add_argument("--port", type=int, default=DB_CONFIG["port"])
    parser.add_argument("--workers", type=int, default=NUM_WORKERS)
    parser.add_argument("--latency", type=str, default="0ms",
                        help="Network latency label (e.g., '0ms', '10ms', '25ms', '50ms', '100ms')")
    args = parser.parse_args()

    DB_CONFIG["host"] = args.host
    DB_CONFIG["port"] = args.port
    num_workers = args.workers
    latency_label = args.latency
    total_duration = sum(p[2] for p in TPS_SCHEDULE)

    print("=" * 70)
    print("  PostgreSQL Logical Replication - Load Generator")
    print("  Experiment: Network Latency")
    print("=" * 70)
    print(f"  Host:       {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Database:   {DB_CONFIG['dbname']}")
    print(f"  Workers:    {num_workers}")
    print(f"  Latency:    {latency_label}")
    print(f"  Total time: {total_duration}s ({total_duration // 60} min)")
    print(f"\n  TPS Schedule:")
    cumulative = 0
    for name, tps, dur in TPS_SCHEDULE:
        m1, s1 = divmod(cumulative, 60)
        m2, s2 = divmod(cumulative + dur, 60)
        pw = tps // num_workers
        print(f"    {m1}:{s1:02d}-{m2}:{s2:02d}  {num_workers:>2} workers | {tps:>5,} TPS total ({pw:>5,}/worker)")
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
        for phase_idx, (phase_name, target_tps, duration) in enumerate(TPS_SCHEDULE):
            if master_stop.is_set():
                break

            pw = target_tps // num_workers
            print(f"\n  {'='*60}")
            print(f"  {phase_name}")
            print(f"  {num_workers} workers | {target_tps:,} TPS ({pw:,}/worker) | Latency: {latency_label} | {duration}s")
            print(f"  {'='*60}")

            write_status(phase_name, target_tps, num_workers, latency_label)

            # Each phase gets a fresh stop event
            phase_stop = Event()
            processes = []

            # Spawn workers with ALL config as direct arguments
            for wid in range(num_workers):
                p = Process(
                    target=worker,
                    args=(wid, num_workers, target_tps, phase_stop, total_inserts)
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
                        f"Rows: {current_total:>10,} | "
                        f"Avg TPS: {actual_tps:>7,.0f} | "
                        f"Target: {target_tps:>5,} | "
                        f"Latency: {latency_label:>5s} | "
                        f"Left: {remaining}s"
                    )
                write_status(phase_name, target_tps, num_workers, latency_label)
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
        write_status("COMPLETED", 0, 0, latency_label)
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
        print(f"  Workers:     {num_workers}")
        print(f"  Latency:     {latency_label}")
        print(f"  Inserts:     {final_total:,}")
        print(f"  Table rows:  {final_count}")
        print(f"  New rows:    {new_rows:,}")
        if total_elapsed > 0:
            print(f"  Avg TPS:     {final_total / total_elapsed:,.0f}")
        print(f"{'='*70}")

        try:
            os.remove(STATUS_FILE)
        except Exception:
            pass


if __name__ == "__main__":
    main()
