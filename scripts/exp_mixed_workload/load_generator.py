#!/usr/bin/env python3
"""
Mixed Workload Load Generator for Replication Lag Testing

This script generates a mixed workload (INSERT/UPDATE/DELETE) against a PostgreSQL
publisher database with increasing concurrency and TPS across phases.

The experiment tests how different workload mixes affect replication lag compared
to pure INSERT-only baselines.

Features:
- Multiprocessing with shared state for phase coordination
- Rate limiting using token bucket algorithm per worker
- Configurable INSERT/UPDATE/DELETE ratios per phase
- Graceful phase transitions (terminate old workers, spawn new ones)
- Maintains recently inserted IDs for efficient UPDATE/DELETE operations
- Real-time tracking of transaction counts by type
- Status file output for monitor coordination

Schedule:
  Phase 1: 1 user, 100 TPS, INSERT only (120s)
  Phase 2: 4 users, 500 TPS, INSERT only (120s)
  Phase 3: 8 users, 1000 TPS, 70/20/10 mix (120s)
  Phase 4: 16 users, 2500 TPS, 70/20/10 mix (120s)
  Phase 5: 32 users, 5000 TPS, 70/20/10 mix (120s)
  Phase 6: 64 users, 5000 TPS, 50/30/20 mix (120s)
Total: 12 minutes
"""

import argparse
import atexit
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from collections import deque
from datetime import datetime
from typing import List, Tuple, Dict, Optional

import psycopg2
import psycopg2.extras


# Global configuration
SCHEDULE: List[Tuple[str, int, int, float, float, float, int]] = [
    ("Phase 1: 1 user, 100 TPS, INSERT only", 1, 100, 1.0, 0.0, 0.0, 120),
    ("Phase 2: 4 users, 500 TPS, INSERT only", 4, 500, 1.0, 0.0, 0.0, 120),
    ("Phase 3: 8 users, 1000 TPS, 70/20/10 mix", 8, 1000, 0.7, 0.2, 0.1, 120),
    ("Phase 4: 16 users, 2500 TPS, 70/20/10 mix", 16, 2500, 0.7, 0.2, 0.1, 120),
    ("Phase 5: 32 users, 5000 TPS, 70/20/10 mix", 32, 5000, 0.7, 0.2, 0.1, 120),
    ("Phase 6: 64 users, 5000 TPS, 50/30/20 mix", 64, 5000, 0.5, 0.3, 0.2, 120),
]

PUBLISHER_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pub",
    "user": "postgres",
    "password": "Aarush@123",
}

# Shared state for tracking
class SharedState:
    """Thread-safe shared state for cross-process communication."""

    def __init__(self):
        self.lock = mp.Lock()
        self.insert_count = mp.Value('i', 0)
        self.update_count = mp.Value('i', 0)
        self.delete_count = mp.Value('i', 0)
        self.error_count = mp.Value('i', 0)
        self.current_phase_name = mp.Array('c', 256)
        self.current_target_tps = mp.Value('i', 0)
        self.current_workers = mp.Value('i', 0)
        self.current_insert_ratio = mp.Value('d', 0.0)
        self.current_update_ratio = mp.Value('d', 0.0)
        self.current_delete_ratio = mp.Value('d', 0.0)

    def increment_insert(self):
        with self.lock:
            self.insert_count.value += 1

    def increment_update(self):
        with self.lock:
            self.update_count.value += 1

    def increment_delete(self):
        with self.lock:
            self.delete_count.value += 1

    def increment_error(self):
        with self.lock:
            self.error_count.value += 1

    def get_counts(self) -> Tuple[int, int, int, int]:
        with self.lock:
            return (
                self.insert_count.value,
                self.update_count.value,
                self.delete_count.value,
                self.error_count.value,
            )

    def update_phase(self, phase_name: str, target_tps: int, num_workers: int,
                     insert_ratio: float, update_ratio: float, delete_ratio: float):
        with self.lock:
            self.current_phase_name.value = phase_name.encode('utf-8')[:255]
            self.current_target_tps.value = target_tps
            self.current_workers.value = num_workers
            self.current_insert_ratio.value = insert_ratio
            self.current_update_ratio.value = update_ratio
            self.current_delete_ratio.value = delete_ratio

    def get_phase_info(self) -> Tuple[str, int, int, float, float, float]:
        with self.lock:
            return (
                self.current_phase_name.value.decode('utf-8'),
                self.current_target_tps.value,
                self.current_workers.value,
                self.current_insert_ratio.value,
                self.current_update_ratio.value,
                self.current_delete_ratio.value,
            )


def generate_payload(size: int) -> str:
    """
    Generate a random text payload of specified size using fast method.

    Args:
        size: Number of characters to generate

    Returns:
        Random string of specified length
    """
    return os.urandom((size + 1) // 2).hex()[:size]


class TokenBucket:
    """
    Simple token bucket rate limiter.

    Allows smooth distribution of requests over time based on target rate.
    """

    def __init__(self, capacity: float, refill_rate: float):
        """
        Initialize token bucket.

        Args:
            capacity: Maximum tokens in bucket
            refill_rate: Tokens refilled per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        """
        Attempt to consume tokens.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if consumption successful, False otherwise
        """
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def wait_for_token(self, tokens: float = 1.0):
        """
        Wait until token is available, then consume it.

        Args:
            tokens: Number of tokens to consume
        """
        while not self.consume(tokens):
            time.sleep(0.001)


def worker_process(worker_id: int,
                   phase_name: str,
                   target_tps: int,
                   num_workers: int,
                   insert_ratio: float,
                   update_ratio: float,
                   delete_ratio: float,
                   duration_seconds: int,
                   shared_state: SharedState):
    """
    Worker process that executes transactions.

    Each worker:
    - Connects to the publisher database
    - Rate limits to (target_tps / num_workers)
    - Randomly selects transaction type based on ratios
    - Maintains a queue of recent IDs for UPDATE/DELETE operations
    - Gracefully handles database errors

    Args:
        worker_id: Unique identifier for this worker
        phase_name: Human-readable phase name
        target_tps: Target transactions per second for entire phase
        num_workers: Number of concurrent workers in this phase
        insert_ratio: Fraction of transactions that are INSERTs
        update_ratio: Fraction of transactions that are UPDATEs
        delete_ratio: Fraction of transactions that are DELETEs
        duration_seconds: How long to run (in seconds)
        shared_state: Shared state object for cross-process communication
    """
    try:
        conn = psycopg2.connect(**PUBLISHER_CONN_PARAMS)
        cursor = conn.cursor()

        # Rate limit: each worker gets equal share of target TPS
        per_worker_tps = target_tps / num_workers
        bucket = TokenBucket(capacity=2.0, refill_rate=per_worker_tps)

        # Keep track of recently inserted IDs for UPDATE/DELETE operations
        recent_ids = deque(maxlen=100)

        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed >= duration_seconds:
                break

            # Wait for token availability
            bucket.wait_for_token(1.0)

            try:
                # Decide transaction type
                rand = random.random()
                if rand < insert_ratio:
                    # INSERT transaction
                    payload = generate_payload(100)
                    big_payload = generate_payload(1000)
                    cursor.execute(
                        "INSERT INTO ingest_data (payload, big_payload) VALUES (%s, %s) RETURNING id",
                        (payload, big_payload)
                    )
                    row_id = cursor.fetchone()[0]
                    recent_ids.append(row_id)
                    conn.commit()
                    shared_state.increment_insert()

                elif rand < insert_ratio + update_ratio:
                    # UPDATE transaction
                    if recent_ids:
                        target_id = random.choice(list(recent_ids))
                        new_payload = generate_payload(100)
                        cursor.execute(
                            "UPDATE ingest_data SET payload = %s WHERE id = %s",
                            (new_payload, target_id)
                        )
                        conn.commit()
                        shared_state.increment_update()
                    # else: skip if no rows available

                else:
                    # DELETE transaction
                    if recent_ids:
                        target_id = recent_ids[0]  # Remove oldest ID
                        recent_ids.remove(target_id)
                        cursor.execute(
                            "DELETE FROM ingest_data WHERE id = %s",
                            (target_id,)
                        )
                        conn.commit()
                        shared_state.increment_delete()
                    # else: skip if no rows available

            except psycopg2.Error as e:
                conn.rollback()
                shared_state.increment_error()
                # Continue processing despite errors

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Worker {worker_id} fatal error: {e}", file=sys.stderr)
        sys.exit(1)


def write_phase_file(output_dir: str, phase_name: str, target_tps: int, num_workers: int,
                     insert_ratio: float, update_ratio: float, delete_ratio: float):
    """
    Write phase information to status file.

    This file is read by the monitor processes to determine current phase info.

    Args:
        output_dir: Directory to write status file to
        phase_name: Human-readable phase name
        target_tps: Target TPS for this phase
        num_workers: Number of concurrent workers
        insert_ratio: Fraction of INSERTs
        update_ratio: Fraction of UPDATEs
        delete_ratio: Fraction of DELETEs
    """
    filepath = os.path.join(output_dir, "tps_phase.txt")
    try:
        with open(filepath, 'w') as f:
            f.write(f"phase_name={phase_name}\n")
            f.write(f"target_tps={target_tps}\n")
            f.write(f"num_workers={num_workers}\n")
            f.write(f"insert_ratio={insert_ratio}\n")
            f.write(f"update_ratio={update_ratio}\n")
            f.write(f"delete_ratio={delete_ratio}\n")
            f.write(f"timestamp={datetime.now().isoformat()}\n")
    except IOError as e:
        print(f"Warning: Could not write phase file: {e}", file=sys.stderr)


def print_stats(shared_state: SharedState, elapsed: float):
    """
    Print current transaction statistics.

    Args:
        shared_state: Shared state object
        elapsed: Elapsed time in seconds
    """
    inserts, updates, deletes, errors = shared_state.get_counts()
    total_txns = inserts + updates + deletes
    tps = total_txns / elapsed if elapsed > 0 else 0

    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
          f"Elapsed: {elapsed:.1f}s | "
          f"Total TXNs: {total_txns} ({tps:.1f} TPS) | "
          f"INS: {inserts} | UPD: {updates} | DEL: {deletes} | "
          f"Errors: {errors}")


def main():
    """Main entry point for load generator."""
    parser = argparse.ArgumentParser(
        description="Mixed workload load generator for PostgreSQL replication lag testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example: python3 load_generator.py --output-dir /tmp/results"
    )
    parser.add_argument("--host", default="localhost",
                       help="Publisher host (default: localhost)")
    parser.add_argument("--port", type=int, default=5432,
                       help="Publisher port (default: 5432)")
    parser.add_argument("--output-dir", default=".",
                       help="Directory for status files (default: current directory)")

    args = parser.parse_args()

    # Update connection params if specified
    PUBLISHER_CONN_PARAMS["host"] = args.host
    PUBLISHER_CONN_PARAMS["port"] = args.port

    # Create output directory if needed
    os.makedirs(args.output_dir, exist_ok=True)

    shared_state = SharedState()
    active_processes: List[mp.Process] = []
    master_stop = mp.Event()

    def cleanup():
        """Terminate all active processes on exit."""
        print("\nShutting down...", file=sys.stderr)
        for proc in active_processes:
            if proc.is_alive():
                proc.terminate()
        deadline = time.time() + 3
        for proc in active_processes:
            remaining = max(0.1, deadline - time.time())
            proc.join(timeout=remaining)
        for proc in active_processes:
            if proc.is_alive():
                proc.kill()

    atexit.register(cleanup)

    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\nInterrupt received, cleaning up...", file=sys.stderr)
        master_stop.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        print(f"Starting mixed workload experiment ({len(SCHEDULE)} phases)")
        print(f"Publisher: {args.host}:{args.port}")
        print(f"Output directory: {args.output_dir}\n")

        overall_start = time.time()

        for phase_idx, (phase_name, num_workers, target_tps,
                        insert_ratio, update_ratio, delete_ratio, duration) in enumerate(SCHEDULE, 1):

            if master_stop.is_set():
                break

            print(f"\n{'='*70}")
            print(f"PHASE {phase_idx}: {phase_name}")
            print(f"Workers: {num_workers} | Target TPS: {target_tps}")
            print(f"Ratios: INSERT {insert_ratio:.0%} | UPDATE {update_ratio:.0%} | DELETE {delete_ratio:.0%}")
            print(f"Duration: {duration}s")
            print(f"{'='*70}\n")

            # Update shared state
            shared_state.update_phase(phase_name, target_tps, num_workers,
                                     insert_ratio, update_ratio, delete_ratio)

            # Write phase info to file
            write_phase_file(args.output_dir, phase_name, target_tps, num_workers,
                           insert_ratio, update_ratio, delete_ratio)

            # Terminate previous workers
            for proc in active_processes:
                if proc.is_alive():
                    proc.terminate()
            deadline = time.time() + 3
            for proc in active_processes:
                remaining = max(0.1, deadline - time.time())
                proc.join(timeout=remaining)
            for proc in active_processes:
                if proc.is_alive():
                    proc.kill()
            active_processes.clear()

            # Spawn new workers for this phase
            for worker_id in range(num_workers):
                proc = mp.Process(
                    target=worker_process,
                    args=(worker_id, phase_name, target_tps, num_workers,
                          insert_ratio, update_ratio, delete_ratio, duration, shared_state)
                )
                proc.start()
                active_processes.append(proc)

            # Monitor phase execution
            phase_start = time.time()
            last_print = phase_start

            while not master_stop.is_set():
                time.sleep(0.1)
                elapsed = time.time() - phase_start

                if elapsed >= duration:
                    break

                # Print stats every 10 seconds
                if time.time() - last_print >= 10:
                    print_stats(shared_state, elapsed)
                    last_print = time.time()

            # Final stats for phase
            print_stats(shared_state, duration)
            print(f"Phase {phase_idx} complete\n")

        # Wait for final workers to finish
        for proc in active_processes:
            if proc.is_alive():
                proc.join()

        total_elapsed = time.time() - overall_start
        inserts, updates, deletes, errors = shared_state.get_counts()
        total_txns = inserts + updates + deletes

        print(f"\n{'='*70}")
        print("EXPERIMENT COMPLETE")
        print(f"{'='*70}")
        print(f"Total runtime: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
        print(f"Total transactions: {total_txns}")
        print(f"  INSERTs: {inserts}")
        print(f"  UPDATEs: {updates}")
        print(f"  DELETEs: {deletes}")
        print(f"  Errors: {errors}")
        print(f"Overall TPS: {total_txns/total_elapsed:.1f}")
        print(f"{'='*70}\n")

    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
