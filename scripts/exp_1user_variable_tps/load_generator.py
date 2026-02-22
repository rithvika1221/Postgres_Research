#!/usr/bin/env python3
"""
Load Generator — 1 User, Variable Throughput
Experiment: 1 user (-c 1 -j 1) with TPS stepping every 2 minutes

TPS Schedule:
  Phase 1 (0:00 - 2:00)  →  100 TPS
  Phase 2 (2:00 - 4:00)  →  500 TPS
  Phase 3 (4:00 - 6:00)  → 1000 TPS
  Phase 4 (6:00 - 8:00)  → 2500 TPS
  Phase 5 (8:00 - 10:00) → 5000 TPS

Single worker rate-limits INSERTs to hit target TPS per phase.
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

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


# ─── Configuration ───────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pub",
    "user": "postgres",
    "password": "Aarush@123",
}

PAYLOAD_SIZE = 100         # characters for payload column
BIG_PAYLOAD_SIZE = 1000    # characters for big_payload column

# TPS schedule: (phase_name, target_tps, duration_seconds)
TPS_SCHEDULE = [
    ("Phase 1: 100 TPS",   100,  120),
    ("Phase 2: 500 TPS",   500,  120),
    ("Phase 3: 1000 TPS", 1000,  120),
    ("Phase 4: 2500 TPS", 2500,  120),
    ("Phase 5: 5000 TPS", 5000,  120),
]

# Status file — monitors read this to know the current TPS phase
STATUS_FILE = "tps_phase.txt"


# ─── Helper Functions ────────────────────────────────────────────────────────

def random_string(length):
    """Generate a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def get_connection():
    """Create and return a new database connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def write_status(phase_name, target_tps, phase_num, total_phases, start_time):
    """Write current TPS phase info to status file for monitors to read."""
    elapsed = time.time() - start_time
    with open(STATUS_FILE, 'w') as f:
        f.write(f"{phase_name}\n")
        f.write(f"target_tps={target_tps}\n")
        f.write(f"phase={phase_num}/{total_phases}\n")
        f.write(f"elapsed_seconds={int(elapsed)}\n")
        f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


def calculate_batch_size(target_tps):
    """
    Choose optimal batch size based on target TPS.
    Larger batches for higher TPS to reduce per-statement overhead.
    """
    if target_tps <= 100:
        return 10       # 10 batches/sec at 100 TPS
    elif target_tps <= 500:
        return 25       # 20 batches/sec at 500 TPS
    elif target_tps <= 1000:
        return 50       # 20 batches/sec at 1000 TPS
    elif target_tps <= 2500:
        return 100      # 25 batches/sec at 2500 TPS
    else:
        return 200      # 25 batches/sec at 5000 TPS


def run_phase(conn, phase_name, target_tps, duration_seconds, phase_num,
              total_phases, experiment_start):
    """
    Run a single TPS phase: rate-limited INSERTs for the given duration.
    Uses a token-bucket approach for smooth rate limiting.
    """
    cur = conn.cursor()
    batch_size = calculate_batch_size(target_tps)
    phase_inserts = 0
    phase_start = time.time()
    phase_end = phase_start + duration_seconds

    # Pre-generate a pool of values to reduce per-row overhead
    values_pool = []
    pool_size = max(batch_size * 10, 1000)
    for _ in range(pool_size):
        payload = random_string(PAYLOAD_SIZE)
        big_payload = random_string(BIG_PAYLOAD_SIZE)
        values_pool.append((payload, big_payload))

    pool_index = 0

    print(f"\n  {'='*55}")
    print(f"  {phase_name}")
    print(f"  Target: {target_tps:,} TPS | Batch: {batch_size} rows | Duration: {duration_seconds}s")
    print(f"  {'='*55}")

    # Token bucket: we track how many inserts we "owe"
    tokens = 0.0
    last_token_time = time.time()

    # Progress tracking
    second_start = time.time()
    second_inserts = 0
    actual_tps_history = []

    while time.time() < phase_end:
        now = time.time()

        # Add tokens based on elapsed time
        elapsed_since_token = now - last_token_time
        tokens += elapsed_since_token * target_tps
        last_token_time = now

        # Cap tokens to prevent burst after delays
        tokens = min(tokens, target_tps * 2)

        if tokens >= batch_size:
            # Build and execute batch INSERT
            batch_values = []
            for _ in range(batch_size):
                batch_values.append(values_pool[pool_index % pool_size])
                pool_index += 1

            args_str = ','.join(
                cur.mogrify("(%s, %s)", v).decode('utf-8')
                for v in batch_values
            )
            cur.execute(
                f"INSERT INTO ingest_data (payload, big_payload) VALUES {args_str}"
            )
            conn.commit()

            tokens -= batch_size
            phase_inserts += batch_size
            second_inserts += batch_size

        else:
            # Sleep a tiny bit to avoid busy-waiting
            sleep_needed = (batch_size - tokens) / target_tps
            time.sleep(min(sleep_needed, 0.01))

        # Print stats every second
        if time.time() - second_start >= 1.0:
            actual_tps = second_inserts / (time.time() - second_start)
            actual_tps_history.append(actual_tps)
            elapsed_phase = int(time.time() - phase_start)
            remaining = max(0, int(phase_end - time.time()))

            # Only print every 5 seconds to avoid flooding
            if elapsed_phase % 5 == 0 or elapsed_phase <= 2:
                print(
                    f"    [{elapsed_phase:>3}s / {duration_seconds}s] "
                    f"Actual TPS: {actual_tps:>7,.0f} | "
                    f"Target: {target_tps:>5,} | "
                    f"Phase rows: {phase_inserts:>10,} | "
                    f"Remaining: {remaining}s"
                )

            second_start = time.time()
            second_inserts = 0

        # Update status file every 5 seconds
        if int(time.time()) % 5 == 0:
            write_status(phase_name, target_tps, phase_num, total_phases,
                        experiment_start)

    # Phase summary
    phase_elapsed = time.time() - phase_start
    avg_tps = phase_inserts / phase_elapsed if phase_elapsed > 0 else 0

    print(f"\n  Phase Complete: {phase_inserts:,} rows in {phase_elapsed:.1f}s "
          f"(avg {avg_tps:,.0f} TPS)")

    return phase_inserts


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="1-user variable TPS load generator"
    )
    parser.add_argument(
        "--host", type=str, default=DB_CONFIG["host"],
        help=f"Database host (default: {DB_CONFIG['host']})"
    )
    parser.add_argument(
        "--port", type=int, default=DB_CONFIG["port"],
        help=f"Database port (default: {DB_CONFIG['port']})"
    )
    args = parser.parse_args()

    DB_CONFIG["host"] = args.host
    DB_CONFIG["port"] = args.port

    total_duration = sum(phase[2] for phase in TPS_SCHEDULE)

    print("=" * 65)
    print("  PostgreSQL Logical Replication — Load Generator")
    print("  Experiment: 1 user (-c 1 -j 1), Variable Throughput")
    print("=" * 65)
    print(f"  Host:         {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Database:     {DB_CONFIG['dbname']}")
    print(f"  Table:        ingest_data")
    print(f"  Workers:      1")
    print(f"  Total time:   {total_duration}s ({total_duration // 60} min)")
    print(f"  Status file:  {os.path.abspath(STATUS_FILE)}")
    print(f"  TPS Schedule:")
    cumulative = 0
    for name, tps, dur in TPS_SCHEDULE:
        start_min = cumulative // 60
        start_sec = cumulative % 60
        end_min = (cumulative + dur) // 60
        end_sec = (cumulative + dur) % 60
        print(f"    {start_min}:{start_sec:02d} - {end_min}:{end_sec:02d}  →  "
              f"{tps:>5,} TPS  ({name})")
        cumulative += dur
    print("=" * 65)

    # Verify connection
    print("\n[1/3] Verifying database connection...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM ingest_data")
        initial_count = cur.fetchone()[0]
        print(f"  Connected. Current row count: {initial_count:,}")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # Verify publication
    print("[2/3] Verifying publication 'mypub'...")
    try:
        cur.execute("SELECT pubname FROM pg_publication WHERE pubname = 'mypub'")
        pub = cur.fetchone()
        if pub:
            print(f"  Publication '{pub[0]}' exists.")
        else:
            print("  WARNING: Publication 'mypub' not found!")
        conn.commit()
    except Exception as e:
        print(f"  WARNING: {e}")

    # Run phases
    print(f"[3/3] Starting experiment at {datetime.now().strftime('%H:%M:%S')}...")
    print(f"  End time: {(datetime.now() + timedelta(seconds=total_duration)).strftime('%H:%M:%S')}")

    experiment_start = time.time()
    grand_total = 0

    # Write initial status
    write_status("STARTING", 0, 0, len(TPS_SCHEDULE), experiment_start)

    try:
        for i, (phase_name, target_tps, duration) in enumerate(TPS_SCHEDULE, 1):
            write_status(phase_name, target_tps, i, len(TPS_SCHEDULE),
                        experiment_start)
            phase_rows = run_phase(
                conn, phase_name, target_tps, duration,
                i, len(TPS_SCHEDULE), experiment_start
            )
            grand_total += phase_rows

    except KeyboardInterrupt:
        print("\n\n  Ctrl+C — stopping experiment...")

    finally:
        # Write final status
        write_status("COMPLETED", 0, len(TPS_SCHEDULE), len(TPS_SCHEDULE),
                    experiment_start)

        # Final row count
        try:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM ingest_data")
            final_count = cur.fetchone()[0]
            conn.commit()
            new_rows = final_count - initial_count
        except Exception:
            final_count = "N/A"
            new_rows = grand_total

        conn.close()

        total_elapsed = time.time() - experiment_start

        print("\n" + "=" * 65)
        print("  EXPERIMENT COMPLETE")
        print("=" * 65)
        print(f"  Duration:         {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print(f"  Total inserts:    {grand_total:,}")
        print(f"  Rows in table:    {final_count}")
        print(f"  New rows:         {new_rows:,}")
        if total_elapsed > 0:
            print(f"  Overall avg TPS:  {grand_total / total_elapsed:,.0f}")
        print(f"\n  Phase breakdown:")
        for name, tps, dur in TPS_SCHEDULE:
            print(f"    {name:25s}  target={tps:>5,} TPS  duration={dur}s")
        print("=" * 65)

        # Cleanup status file
        try:
            os.remove(STATUS_FILE)
        except Exception:
            pass


if __name__ == "__main__":
    main()
