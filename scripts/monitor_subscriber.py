#!/usr/bin/env python3
"""
Subscriber (Replica) Replication Monitor
Monitors logical replication performance from the subscriber side

Run this on your local machine, connects to the subscriber database
Collects: subscription lag, worker activity, apply statistics, conflicts

Usage:
    export PGPASSWORD='your_password'
    python monitor_subscriber.py

Output Files:
    - subscriber_subscription_stats.csv: Subscription lag and status
    - subscriber_activity.csv: Connection and worker activity
    - subscriber_database.csv: Database-level statistics
    - subscriber_conflicts.csv: Replication conflicts
"""

import os
import sys
import time
import csv
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# =============================================================================

# For running on the VM directly (recommended):
SUBSCRIBER_HOST = 'localhost'  # Use localhost when running on the subscriber VM
SUBSCRIBER_PORT = 5432
SUBSCRIBER_DBNAME = 'sub'  # UPDATE THIS if your database name is different
SUBSCRIBER_USER = 'postgres'
SUBSCRIBER_PASSWORD = os.getenv('PGPASSWORD')
SUBSCRIBER_SSLMODE = 'disable'  # Can disable SSL for localhost connections

# For running remotely from your MacBook (alternative):
# SUBSCRIBER_HOST = 'pg-replica2-westus2.postgres.database.azure.com'
# SUBSCRIBER_SSLMODE = 'require'

# Monitoring Configuration
SAMPLE_INTERVAL = 5  # seconds between samples
OUTPUT_DIR = '.'     # directory for CSV files

# =============================================================================
# SQL QUERIES FOR SUBSCRIBER
# =============================================================================

# Subscription statistics and lag
QUERY_SUBSCRIPTION_STATS = """
SELECT
    NOW() as timestamp,
    subname,
    pid,
    leader_pid,
    relid,
    received_lsn,
    last_msg_send_time,
    last_msg_receipt_time,
    latest_end_lsn,
    latest_end_time,
    pg_wal_lsn_diff(latest_end_lsn, received_lsn) as lag_bytes
FROM pg_stat_subscription
ORDER BY subname;
"""

# Subscription worker details
QUERY_SUBSCRIPTION_WORKERS = """
SELECT
    NOW() as timestamp,
    s.subname,
    s.pid,
    s.leader_pid,
    a.state,
    a.wait_event_type,
    a.wait_event,
    a.query_start,
    a.state_change,
    a.backend_start,
    EXTRACT(EPOCH FROM (NOW() - a.query_start)) as query_duration_sec,
    EXTRACT(EPOCH FROM (NOW() - a.state_change)) as state_duration_sec
FROM pg_stat_subscription s
LEFT JOIN pg_stat_activity a ON s.pid = a.pid
ORDER BY s.subname;
"""

# Activity statistics
QUERY_ACTIVITY_STATS = """
SELECT
    NOW() as timestamp,
    COUNT(*) as total_connections,
    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
    COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting_connections,
    COUNT(*) FILTER (WHERE wait_event_type = 'IO') as io_waiting,
    COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as lock_waiting,
    COUNT(*) FILTER (WHERE backend_type = 'logical replication worker') as logical_rep_workers,
    COUNT(*) FILTER (WHERE backend_type = 'logical replication launcher') as logical_rep_launcher
FROM pg_stat_activity;
"""

# Database statistics
QUERY_DATABASE_STATS = """
SELECT
    NOW() as timestamp,
    datname,
    numbackends,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    conflicts,
    temp_files,
    temp_bytes,
    deadlocks,
    checksum_failures,
    blk_read_time,
    blk_write_time,
    stats_reset
FROM pg_stat_database
WHERE datname = current_database();
"""

# Replication conflicts (subscription-specific)
QUERY_CONFLICTS = """
SELECT
    NOW() as timestamp,
    datname,
    confl_tablespace,
    confl_lock,
    confl_snapshot,
    confl_bufferpin,
    confl_deadlock
FROM pg_stat_database_conflicts
WHERE datname = current_database();
"""

# Table-level statistics (to see apply progress)
QUERY_TABLE_STATS = """
SELECT
    NOW() as timestamp,
    schemaname,
    relname,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC
LIMIT 10;
"""

# Subscription configuration
QUERY_SUBSCRIPTION_CONFIG = """
SELECT
    NOW() as timestamp,
    s.subname,
    s.subconninfo,
    s.subenabled,
    s.subslotname,
    s.subsynccommit,
    s.subpublications
FROM pg_subscription s
ORDER BY s.subname;
"""

# =============================================================================
# CSV FILE CONFIGURATION
# =============================================================================

CSV_FILES = {
    'subscription_stats': {
        'filename': f'{OUTPUT_DIR}/subscriber_subscription_stats.csv',
        'fieldnames': ['timestamp', 'subname', 'pid', 'leader_pid', 'relid', 'received_lsn',
                      'last_msg_send_time', 'last_msg_receipt_time', 'latest_end_lsn',
                      'latest_end_time', 'lag_bytes', 'lag_mb']
    },
    'subscription_workers': {
        'filename': f'{OUTPUT_DIR}/subscriber_workers.csv',
        'fieldnames': ['timestamp', 'subname', 'pid', 'leader_pid', 'state', 'wait_event_type',
                      'wait_event', 'query_start', 'state_change', 'backend_start',
                      'query_duration_sec', 'state_duration_sec']
    },
    'activity': {
        'filename': f'{OUTPUT_DIR}/subscriber_activity.csv',
        'fieldnames': ['timestamp', 'total_connections', 'active_connections', 'idle_connections',
                      'idle_in_transaction', 'waiting_connections', 'io_waiting', 'lock_waiting',
                      'logical_rep_workers', 'logical_rep_launcher']
    },
    'database': {
        'filename': f'{OUTPUT_DIR}/subscriber_database.csv',
        'fieldnames': ['timestamp', 'datname', 'numbackends', 'xact_commit', 'xact_rollback',
                      'blks_read', 'blks_hit', 'tup_returned', 'tup_fetched', 'tup_inserted',
                      'tup_updated', 'tup_deleted', 'conflicts', 'temp_files', 'temp_bytes',
                      'deadlocks', 'checksum_failures', 'blk_read_time', 'blk_write_time',
                      'stats_reset', 'tps', 'cache_hit_ratio', 'inserts_per_sec',
                      'updates_per_sec', 'deletes_per_sec']
    },
    'conflicts': {
        'filename': f'{OUTPUT_DIR}/subscriber_conflicts.csv',
        'fieldnames': ['timestamp', 'datname', 'confl_tablespace', 'confl_lock', 'confl_snapshot',
                      'confl_bufferpin', 'confl_deadlock', 'total_conflicts']
    },
    'table_stats': {
        'filename': f'{OUTPUT_DIR}/subscriber_table_stats.csv',
        'fieldnames': ['timestamp', 'schemaname', 'relname', 'n_tup_ins', 'n_tup_upd', 'n_tup_del',
                      'n_live_tup', 'n_dead_tup', 'last_vacuum', 'last_autovacuum',
                      'last_analyze', 'last_autoanalyze']
    },
    'subscription_config': {
        'filename': f'{OUTPUT_DIR}/subscriber_subscription_config.csv',
        'fieldnames': ['timestamp', 'subname', 'subconninfo', 'subenabled', 'subslotname',
                      'subsynccommit', 'subpublications']
    }
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def init_csv_files():
    """Initialize all CSV files with headers"""
    for key, config in CSV_FILES.items():
        try:
            with open(config['filename'], 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=config['fieldnames'])
                writer.writeheader()
            print(f"✓ Initialized {config['filename']}")
        except Exception as e:
            print(f"✗ Error initializing {config['filename']}: {e}")
            sys.exit(1)

def write_to_csv(file_key, rows):
    """Write rows to specified CSV file"""
    if not rows:
        return

    config = CSV_FILES[file_key]
    try:
        with open(config['filename'], 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=config['fieldnames'])
            for row in rows:
                # Filter only fields that are in fieldnames
                filtered_row = {k: v for k, v in row.items() if k in config['fieldnames']}
                writer.writerow(filtered_row)
    except Exception as e:
        print(f"✗ Error writing to {config['filename']}: {e}")

def connect_db():
    """Establish database connection to subscriber"""
    try:
        conn = psycopg2.connect(
            host=SUBSCRIBER_HOST,
            port=SUBSCRIBER_PORT,
            dbname=SUBSCRIBER_DBNAME,
            user=SUBSCRIBER_USER,
            password=SUBSCRIBER_PASSWORD,
            sslmode=SUBSCRIBER_SSLMODE
        )
        print(f"✓ Connected to Subscriber: {SUBSCRIBER_HOST}/{SUBSCRIBER_DBNAME}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to Subscriber: {e}")
        return None

def execute_query(conn, query):
    """Execute query and return results as list of dicts"""
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            return [dict(row) for row in results]
    except Exception as e:
        print(f"✗ Query error: {e}")
        return []

# =============================================================================
# METRIC CALCULATIONS
# =============================================================================

prev_db_stats = None
prev_conflicts = None

def calculate_subscription_deltas(current_stats):
    """Add calculated fields to subscription stats"""
    if not current_stats:
        return current_stats

    result = []
    for row in current_stats:
        row_copy = dict(row)
        # Add lag in MB
        row_copy['lag_mb'] = row['lag_bytes'] / (1024 * 1024) if row['lag_bytes'] else 0
        result.append(row_copy)

    return result

def calculate_db_deltas(current_stats):
    """Calculate transaction rates and cache hit ratio"""
    global prev_db_stats

    if not current_stats:
        return current_stats

    result = []
    for row in current_stats:
        row_copy = dict(row)

        # Calculate cache hit ratio
        total_reads = row['blks_hit'] + row['blks_read']
        if total_reads > 0:
            row_copy['cache_hit_ratio'] = (row['blks_hit'] / total_reads) * 100
        else:
            row_copy['cache_hit_ratio'] = 0

        if prev_db_stats:
            for prev_row in prev_db_stats:
                time_delta = (row['timestamp'] - prev_row['timestamp']).total_seconds()

                if time_delta > 0:
                    commits_delta = row['xact_commit'] - prev_row['xact_commit']
                    rollbacks_delta = row['xact_rollback'] - prev_row['xact_rollback']

                    row_copy['tps'] = (commits_delta + rollbacks_delta) / time_delta
                    row_copy['inserts_per_sec'] = (row['tup_inserted'] - prev_row['tup_inserted']) / time_delta
                    row_copy['updates_per_sec'] = (row['tup_updated'] - prev_row['tup_updated']) / time_delta
                    row_copy['deletes_per_sec'] = (row['tup_deleted'] - prev_row['tup_deleted']) / time_delta
                else:
                    row_copy['tps'] = 0
                    row_copy['inserts_per_sec'] = 0
                    row_copy['updates_per_sec'] = 0
                    row_copy['deletes_per_sec'] = 0
        else:
            row_copy['tps'] = 0
            row_copy['inserts_per_sec'] = 0
            row_copy['updates_per_sec'] = 0
            row_copy['deletes_per_sec'] = 0

        result.append(row_copy)

    prev_db_stats = current_stats
    return result

def calculate_conflicts_deltas(current_stats):
    """Add total conflicts field"""
    if not current_stats:
        return current_stats

    result = []
    for row in current_stats:
        row_copy = dict(row)
        row_copy['total_conflicts'] = (
            row.get('confl_tablespace', 0) +
            row.get('confl_lock', 0) +
            row.get('confl_snapshot', 0) +
            row.get('confl_bufferpin', 0) +
            row.get('confl_deadlock', 0)
        )
        result.append(row_copy)

    return result

# =============================================================================
# MAIN MONITORING LOOP
# =============================================================================

def monitor():
    """Main monitoring function"""

    # Check password
    if not SUBSCRIBER_PASSWORD:
        print("✗ Error: PGPASSWORD environment variable not set")
        print("  Run: export PGPASSWORD='your_password'")
        sys.exit(1)

    print("\n" + "="*70)
    print("PostgreSQL Subscriber Replication Monitor")
    print("="*70)
    print(f"Subscriber: {SUBSCRIBER_HOST}/{SUBSCRIBER_DBNAME}")
    print(f"Sample interval: {SAMPLE_INTERVAL} seconds")
    print(f"Output directory: {OUTPUT_DIR}")
    print("Press Ctrl+C to stop\n")

    # Initialize CSV files
    init_csv_files()

    # Connect to subscriber
    conn = connect_db()

    if not conn:
        print("\n✗ Cannot continue without subscriber connection")
        sys.exit(1)

    print("\n" + "="*70)
    print("Monitoring started... (Ctrl+C to stop)")
    print("="*70 + "\n")

    sample_count = 0

    try:
        while True:
            sample_count += 1
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            print(f"[{timestamp}] Sample #{sample_count}")

            # 1. Subscription statistics and lag
            sub_data = execute_query(conn, QUERY_SUBSCRIPTION_STATS)
            sub_data_with_deltas = calculate_subscription_deltas(sub_data)
            if sub_data_with_deltas:
                write_to_csv('subscription_stats', sub_data_with_deltas)
                for row in sub_data_with_deltas:
                    lag_mb = row.get('lag_mb', 0)
                    print(f"  → Subscription Lag: {lag_mb:.2f} MB | Name: {row['subname']}")
            else:
                print("  → No active subscriptions found")

            # 2. Subscription worker details
            worker_data = execute_query(conn, QUERY_SUBSCRIPTION_WORKERS)
            if worker_data:
                write_to_csv('subscription_workers', worker_data)
                for row in worker_data:
                    state = row.get('state', 'N/A')
                    wait = row.get('wait_event', 'none')
                    print(f"  → Worker {row['pid']}: {state} | Waiting on: {wait}")

            # 3. Activity statistics
            activity_data = execute_query(conn, QUERY_ACTIVITY_STATS)
            if activity_data:
                write_to_csv('activity', activity_data)
                for row in activity_data:
                    print(f"  → Connections: {row['total_connections']} total | {row['active_connections']} active | {row['logical_rep_workers']} workers")

            # 4. Database statistics
            db_data = execute_query(conn, QUERY_DATABASE_STATS)
            db_data_with_deltas = calculate_db_deltas(db_data)
            if db_data_with_deltas:
                write_to_csv('database', db_data_with_deltas)
                for row in db_data_with_deltas:
                    print(f"  → Apply Rate: {row['inserts_per_sec']:.1f} ins/s | {row['updates_per_sec']:.1f} upd/s | Cache Hit: {row['cache_hit_ratio']:.1f}%")

            # 5. Conflicts
            conflicts_data = execute_query(conn, QUERY_CONFLICTS)
            conflicts_with_totals = calculate_conflicts_deltas(conflicts_data)
            if conflicts_with_totals:
                write_to_csv('conflicts', conflicts_with_totals)
                for row in conflicts_with_totals:
                    total = row.get('total_conflicts', 0)
                    if total > 0:
                        print(f"  ⚠ Conflicts: {total} total (lock:{row['confl_lock']}, snapshot:{row['confl_snapshot']}, deadlock:{row['confl_deadlock']})")

            # 6. Table statistics (only collect periodically to reduce overhead)
            if sample_count % 12 == 0:  # Every minute if interval is 5 sec
                table_data = execute_query(conn, QUERY_TABLE_STATS)
                if table_data:
                    write_to_csv('table_stats', table_data)

            # 7. Subscription config (only on first sample)
            if sample_count == 1:
                config_data = execute_query(conn, QUERY_SUBSCRIPTION_CONFIG)
                if config_data:
                    write_to_csv('subscription_config', config_data)
                    print("  → Subscription configuration saved")

            print()  # Blank line

            # Wait for next sample
            time.sleep(SAMPLE_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        print("Monitoring stopped by user")
        print("="*70)

    finally:
        if conn:
            conn.close()
            print("✓ Closed subscriber connection")

        print(f"\n✓ Collected {sample_count} samples")
        print("\nOutput files:")
        for key, config in CSV_FILES.items():
            print(f"  - {config['filename']}")
        print("\nImport these CSV files into Excel for analysis.\n")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    monitor()
