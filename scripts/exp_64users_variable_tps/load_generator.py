#!/usr/bin/env python3
"""
Load Generator — 64 users (-c 64 -j 16), Variable Throughput
Experiment: 64 users (-c 64 -j 16) with TPS stepping every 2 minutes

TPS Schedule:
  Phase 1 (0:00 - 2:00)  ->  100 TPS   (1 TPS per worker)
  Phase 2 (2:00 - 4:00)  ->  500 TPS   (7 TPS per worker)
  Phase 3 (4:00 - 6:00)  -> 1000 TPS   (15 TPS per worker)
  Phase 4 (6:00 - 8:00)  -> 2500 TPS   (39 TPS per worker)
  Phase 5 (8:00 - 10:00) -> 5000 TPS   (78 TPS per worker)

64 parallel workers split the target TPS evenly between them.
Writes a status file (tps_phase.txt) so monitors can read the current phase.

Run on: PUBLISHER VM
Requires: psycopg2
Usage:   python3 load_generator.py
"""

import os
import sys
import time
import random
import string
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

NUM_WORKERS = 64
PAYLOAD_SIZE = 100
BIG_PAYLOAD_SIZE = 1000

TPS_SCHEDULE = [
    ("Phase 1: 100 TPS",   100,  120),
    ("Phase 2: 500 TPS",   500,  120),
    ("Phase 3: 1000 TPS", 1000,  120),
    ("Phase 4: 2500 TPS", 2500,  120),
    ("Phase 5: 5000 TPS", 5000,  120),
]

STATUS_FILE = "tps_phase.txt"


def random_string(length):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn

def write_status(phase_name, target_tps, phase_num, total_phases, start_time):
    elapsed = time.time() - start_time
    with open(STATUS_FILE, "w") as f:
        f.write(f"{phase_name}\n")
        f.write(f"target_tps={target_tps}\n")
        f.write(f"phase={phase_num}/{total_phases}\n")
        f.write(f"elapsed_seconds={int(elapsed)}\n")
        f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

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


def worker(worker_id, num_workers, stop_event, total_inserts, current_phase_index):
    random.seed(worker_id + int(time.time()))
    conn = None
    local_count = 0

    try:
        conn = get_connection()
        cur = conn.cursor()
        print(f"  [Worker {worker_id}] Connected.")

        pool_size = 2000
        values_pool = []
        for _ in range(pool_size):
            values_pool.append((random_string(PAYLOAD_SIZE), random_string(BIG_PAYLOAD_SIZE)))
        pool_index = 0

        tokens = 0.0
        last_token_time = time.time()
        current_worker_tps = 0
        current_batch_size = 5

        while not stop_event.is_set():
            now = time.time()
            phase_idx = current_phase_index.value
            if phase_idx < len(TPS_SCHEDULE):
                total_tps = TPS_SCHEDULE[phase_idx][1]
                new_worker_tps = total_tps / num_workers
            else:
                break

            if new_worker_tps != current_worker_tps:
                current_worker_tps = new_worker_tps
                current_batch_size = calculate_batch_size(current_worker_tps)
                tokens = 0.0
                last_token_time = now

            elapsed_since_token = now - last_token_time
            tokens += elapsed_since_token * current_worker_tps
            last_token_time = now
            tokens = min(tokens, current_worker_tps * 2)

            if tokens >= current_batch_size:
                batch_values = []
                for _ in range(current_batch_size):
                    batch_values.append(values_pool[pool_index % pool_size])
                    pool_index += 1

                args_str = ",".join(
                    cur.mogrify("(%s, %s)", v).decode("utf-8") for v in batch_values
                )
                cur.execute(
                    f"INSERT INTO ingest_data (payload, big_payload) VALUES {args_str}"
                )
                conn.commit()
                tokens -= current_batch_size
                local_count += current_batch_size
            else:
                if current_worker_tps > 0:
                    sleep_needed = (current_batch_size - tokens) / current_worker_tps
                    time.sleep(min(sleep_needed, 0.05))
                else:
                    time.sleep(0.1)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"  [Worker {worker_id}] ERROR: {e}")
    finally:
        if conn:
            try:
                conn.commit()
                conn.close()
            except Exception:
                pass
        with total_inserts.get_lock():
            total_inserts.value += local_count
        print(f"  [Worker {worker_id}] Finished. Inserted {local_count:,} rows.")


def main():
    parser = argparse.ArgumentParser(description="64 users (-c 64 -j 16) variable TPS load generator")
    parser.add_argument("--host", type=str, default=DB_CONFIG["host"])
    parser.add_argument("--port", type=int, default=DB_CONFIG["port"])
    parser.add_argument("--workers", type=int, default=NUM_WORKERS)
    args = parser.parse_args()

    DB_CONFIG["host"] = args.host
    DB_CONFIG["port"] = args.port
    num_workers = args.workers
    total_duration = sum(p[2] for p in TPS_SCHEDULE)

    print("=" * 65)
    print("  PostgreSQL Logical Replication - Load Generator")
    print("  Experiment: 64 users (-c 64 -j 16), Variable Throughput")
    print("=" * 65)
    print(f"  Host:       {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Database:   {DB_CONFIG['dbname']}")
    print(f"  Workers:    {num_workers}")
    print(f"  Total time: {total_duration}s ({total_duration // 60} min)")
    print(f"\n  TPS Schedule (total -> per worker):")
    cumulative = 0
    for name, tps, dur in TPS_SCHEDULE:
        m1, s1 = divmod(cumulative, 60)
        m2, s2 = divmod(cumulative + dur, 60)
        pw = tps // num_workers
        print(f"    {m1}:{s1:02d} - {m2}:{s2:02d}  ->  {tps:>5,} TPS total ({pw:>5,}/worker)")
        cumulative += dur
    print("=" * 65)

    print("\n[1/3] Verifying database connection...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM ingest_data")
        initial_count = cur.fetchone()[0]
        print(f"  Connected. Row count: {initial_count:,}")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    print("[2/3] Verifying publication...")
    try:
        cur.execute("SELECT pubname FROM pg_publication WHERE pubname = 'mypub'")
        pub = cur.fetchone()
        print(f"  Publication '{pub[0]}' exists." if pub else "  WARNING: not found!")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  WARNING: {e}")

    stop_event = Event()
    total_inserts = Value("i", 0)
    current_phase_index = Value("i", 0)

    print(f"[3/3] Launching {num_workers} workers...")
    experiment_start = time.time()

    processes = []
    for i in range(num_workers):
        p = Process(target=worker, args=(i, num_workers, stop_event, total_inserts, current_phase_index))
        p.start()
        processes.append(p)
        print(f"  Worker {i} started (PID: {p.pid})")

    write_status("STARTING", 0, 0, len(TPS_SCHEDULE), experiment_start)
    print(f"\n  Started at {datetime.now().strftime('%H:%M:%S')}")
    print(f"  End: {(datetime.now() + timedelta(seconds=total_duration)).strftime('%H:%M:%S')}\n")

    try:
        for phase_idx, (phase_name, target_tps, duration) in enumerate(TPS_SCHEDULE):
            current_phase_index.value = phase_idx
            write_status(phase_name, target_tps, phase_idx + 1, len(TPS_SCHEDULE), experiment_start)
            pw = target_tps // num_workers
            print(f"  ===  {phase_name}  |  {target_tps:,} TPS total ({pw:,}/worker)  |  {duration}s  ===")

            phase_start = time.time()
            phase_end = phase_start + duration

            while time.time() < phase_end and not stop_event.is_set():
                elapsed_phase = int(time.time() - phase_start)
                elapsed_total = int(time.time() - experiment_start)
                remaining = max(0, int(phase_end - time.time()))
                with total_inserts.get_lock():
                    current_total = total_inserts.value
                actual_tps = current_total / elapsed_total if elapsed_total > 0 else 0

                if elapsed_phase % 10 == 0:
                    print(
                        f"    [{elapsed_phase:>3}s / {duration}s] "
                        f"Rows: {current_total:>10,} | "
                        f"Avg TPS: {actual_tps:>7,.0f} | "
                        f"Target: {target_tps:>5,} | "
                        f"Left: {remaining}s"
                    )
                write_status(phase_name, target_tps, phase_idx + 1, len(TPS_SCHEDULE), experiment_start)
                time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n  Ctrl+C - stopping...")

    finally:
        stop_event.set()
        current_phase_index.value = len(TPS_SCHEDULE)
        write_status("COMPLETED", 0, len(TPS_SCHEDULE), len(TPS_SCHEDULE), experiment_start)

        for p in processes:
            p.join(timeout=15)
            if p.is_alive():
                p.terminate()

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

        print("\n" + "=" * 65)
        print("  EXPERIMENT COMPLETE")
        print("=" * 65)
        print(f"  Duration:    {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print(f"  Workers:     {num_workers}")
        print(f"  Inserts:     {final_total:,}")
        print(f"  Table rows:  {final_count}")
        print(f"  New rows:    {new_rows:,}")
        if total_elapsed > 0:
            print(f"  Avg TPS:     {final_total / total_elapsed:,.0f}")
        print("=" * 65)

        try:
            os.remove(STATUS_FILE)
        except Exception:
            pass


if __name__ == "__main__":
    main()
