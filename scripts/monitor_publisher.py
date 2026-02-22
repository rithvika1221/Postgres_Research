#!/usr/bin/env python3
"""
Publisher (Primary) Replication Monitor
Monitors logical replication performance from the publisher side

Run this on your local machine, connects to the publisher database
Collects: replication lag, WAL stats, activity, database metrics

Usage:
    export PGPASSWORD='your_password'
    python monitor_publisher.py

Output Files:
    - publisher_replication_lag.csv: Replication lag and LSN positions
    - publisher_wal_stats.csv: WAL generation statistics
    - publisher_activity.csv: Connection and query activity
    - publisher_database.csv: Transaction rates and database statistics
    - publisher_wal_lsn.csv: Current WAL positions
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
PUBLISHER_HOST = 'localhost'  # Use localhost when running on the publisher VM
PUBLISHER_PORT = 5432
PUBLISHER_DBNAME = 'pub'
PUBLISHER_USER = 'postgres'
PUBLISHER_PASSWORD = os.getenv('PGPASSWORD')
PUBLISHER_SSLMODE = 'disable'  # Can disable SSL for localhost connections

# For running remotely from your MacBook (alternative):
# PUBLISHER_HOST = 'pg-source2-westus2.postgres.database.azure.com'
# PUBLISHER_SSLMODE = 'require'

# Monitoring Configuration
SAMPLE_INTERVAL = 5  # seconds between samples
OUTPUT_DIR = '.'     # directory for CSV files

# =============================================================================
# SQL QUERIES FOR PUBLISHER
# =============================================================================

# Replication status from publisher perspective
QUERY_REPLICATION_STATUS = """
SELECT
    NOW() as timestamp,
    application_name,
    client_addr,
    client_hostname,
    client_port,
    backend_start,
    backend_xmin,
    state,
    sync_state,
    sync_priority,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    pg_wal_lsn_diff(sent_lsn, replay_lsn) as total_lag_bytes,
    pg_wal_lsn_diff(sent_lsn, write_lsn) as send_lag_bytes,
    pg_wal_lsn_diff(write_lsn, flush_lsn) as flush_lag_bytes,
    pg_wal_lsn_diff(flush_lsn, replay_lsn) as apply_lag_bytes,
    write_lag,
    flush_lag,
    replay_lag,
    reply_time
FROM pg_stat_replication
ORDER BY application_name;
"""

# WAL generation and writing statistics
QUERY_WAL_STATS = """
SELECT
    NOW() as timestamp,
    wal_records,
    wal_fpi,
    wal_bytes,
    wal_buffers_full,
    wal_write,
    wal_sync,
    wal_write_time,
    wal_sync_time,
    stats_reset
FROM pg_stat_wal;
"""

# Connection and activity statistics
QUERY_ACTIVITY_STATS = """
SELECT
    NOW() as timestamp,
    COUNT(*) as total_connections,
    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
    COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_in_transaction_aborted,
    COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting_connections,
    COUNT(*) FILTER (WHERE wait_event_type = 'IO') as io_waiting,
    COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as lock_waiting,
    COUNT(*) FILTER (WHERE wait_event_type = 'LWLock') as lwlock_waiting,
    COUNT(*) FILTER (WHERE backend_type = 'walsender') as walsender_count,
    COUNT(*) FILTER (WHERE backend_type = 'client backend') as client_backends
FROM pg_stat_activity;
"""

# Database-level statistics
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
    session_time,
    active_time,
    idle_in_transaction_time,
    sessions,
    sessions_abandoned,
    sessions_fatal,
    sessions_killed,
    stats_reset
FROM pg_stat_database
WHERE datname = current_database();
"""

# Current WAL LSN positions
QUERY_WAL_LSN = """
SELECT
    NOW() as timestamp,
    pg_current_wal_lsn() as current_wal_lsn,
    pg_current_wal_insert_lsn() as current_wal_insert_lsn,
    pg_current_wal_flush_lsn() as current_wal_flush_lsn
"""

# Replication slots status
QUERY_REPLICATION_SLOTS = """
SELECT
    NOW() as timestamp,
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as restart_lag_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as confirmed_lag_bytes,
    wal_status,
    safe_wal_size
FROM pg_replication_slots
WHERE slot_type = 'logical'
ORDER BY slot_name;
"""

# =============================================================================
# CSV FILE CONFIGURATION
# =============================================================================

CSV_FILES = {
    'replication_lag': {
        'filename': f'{OUTPUT_DIR}/publisher_replication_lag.csv',
        'fieldnames': ['timestamp', 'application_name', 'client_addr', 'client_hostname',
                      'client_port', 'backend_start', 'backend_xmin', 'state', 'sync_state',
                      'sync_priority', 'sent_lsn', 'write_lsn', 'flush_lsn', 'replay_lsn',
                      'total_lag_bytes', 'send_lag_bytes', 'flush_lag_bytes', 'apply_lag_bytes',
                      'write_lag', 'flush_lag', 'replay_lag', 'reply_time']
    },
    'wal_stats': {
        'filename': f'{OUTPUT_DIR}/publisher_wal_stats.csv',
        'fieldnames': ['timestamp', 'wal_records', 'wal_fpi', 'wal_bytes', 'wal_buffers_full',
                      'wal_write', 'wal_sync', 'wal_write_time', 'wal_sync_time', 'stats_reset',
                      'wal_records_delta', 'wal_bytes_delta', 'wal_bytes_per_sec', 'wal_mb_per_sec']
    },
    'activity': {
        'filename': f'{OUTPUT_DIR}/publisher_activity.csv',
        'fieldnames': ['timestamp', 'total_connections', 'active_connections', 'idle_connections',
                      'idle_in_transaction', 'idle_in_transaction_aborted', 'waiting_connections',
                      'io_waiting', 'lock_waiting', 'lwlock_waiting', 'walsender_count', 'client_backends']
    },
    'database': {
        'filename': f'{OUTPUT_DIR}/publisher_database.csv',
        'fieldnames': ['timestamp', 'datname', 'numbackends', 'xact_commit', 'xact_rollback',
                      'blks_read', 'blks_hit', 'tup_returned', 'tup_fetched', 'tup_inserted',
                      'tup_updated', 'tup_deleted', 'conflicts', 'temp_files', 'temp_bytes',
                      'deadlocks', 'checksum_failures', 'blk_read_time', 'blk_write_time',
                      'session_time', 'active_time', 'idle_in_transaction_time', 'sessions',
                      'sessions_abandoned', 'sessions_fatal', 'sessions_killed', 'stats_reset',
                      'tps', 'cache_hit_ratio', 'inserts_per_sec', 'updates_per_sec', 'deletes_per_sec']
    },
    'wal_lsn': {
        'filename': f'{OUTPUT_DIR}/publisher_wal_lsn.csv',
        'fieldnames': ['timestamp', 'current_wal_lsn', 'current_wal_insert_lsn', 'current_wal_flush_lsn']
    },
    'replication_slots': {
        'filename': f'{OUTPUT_DIR}/publisher_replication_slots.csv',
        'fieldnames': ['timestamp', 'slot_name', 'plugin', 'slot_type', 'database', 'active',
                      'active_pid', 'restart_lsn', 'confirmed_flush_lsn', 'restart_lag_bytes',
                      'confirmed_lag_bytes', 'wal_status', 'safe_wal_size']
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
    """Establish database connection to publisher"""
    try:
        conn = psycopg2.connect(
            host=PUBLISHER_HOST,
            port=PUBLISHER_PORT,
            dbname=PUBLISHER_DBNAME,
            user=PUBLISHER_USER,
            password=PUBLISHER_PASSWORD,
            sslmode=PUBLISHER_SSLMODE
        )
        print(f"✓ Connected to Publisher: {PUBLISHER_HOST}/{PUBLISHER_DBNAME}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to Publisher: {e}")
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

prev_wal_stats = None
prev_db_stats = None
prev_timestamp = None

def calculate_wal_deltas(current_stats):
    """Calculate WAL generation rate and deltas"""
    global prev_wal_stats, prev_timestamp

    if not current_stats:
        return current_stats

    result = []
    for row in current_stats:
        row_copy = dict(row)

        if prev_wal_stats and prev_timestamp:
            time_delta = (row['timestamp'] - prev_timestamp).total_seconds()

            if time_delta > 0:
                for prev_row in prev_wal_stats:
                    wal_records_delta = row['wal_records'] - prev_row['wal_records']
                    wal_bytes_delta = row['wal_bytes'] - prev_row['wal_bytes']

                    row_copy['wal_records_delta'] = wal_records_delta
                    row_copy['wal_bytes_delta'] = wal_bytes_delta
                    row_copy['wal_bytes_per_sec'] = wal_bytes_delta / time_delta
                    row_copy['wal_mb_per_sec'] = (wal_bytes_delta / time_delta) / (1024 * 1024)
            else:
                row_copy['wal_records_delta'] = 0
                row_copy['wal_bytes_delta'] = 0
                row_copy['wal_bytes_per_sec'] = 0
                row_copy['wal_mb_per_sec'] = 0
        else:
            row_copy['wal_records_delta'] = 0
            row_copy['wal_bytes_delta'] = 0
            row_copy['wal_bytes_per_sec'] = 0
            row_copy['wal_mb_per_sec'] = 0

        result.append(row_copy)

    prev_wal_stats = current_stats
    prev_timestamp = current_stats[0]['timestamp'] if current_stats else None

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

# =============================================================================
# MAIN MONITORING LOOP
# =============================================================================

def monitor():
    """Main monitoring function"""

    # Check password
    if not PUBLISHER_PASSWORD:
        print("✗ Error: PGPASSWORD environment variable not set")
        print("  Run: export PGPASSWORD='your_password'")
        sys.exit(1)

    print("\n" + "="*70)
    print("PostgreSQL Publisher Replication Monitor")
    print("="*70)
    print(f"Publisher: {PUBLISHER_HOST}/{PUBLISHER_DBNAME}")
    print(f"Sample interval: {SAMPLE_INTERVAL} seconds")
    print(f"Output directory: {OUTPUT_DIR}")
    print("Press Ctrl+C to stop\n")

    # Initialize CSV files
    init_csv_files()

    # Connect to publisher
    conn = connect_db()

    if not conn:
        print("\n✗ Cannot continue without publisher connection")
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

            # 1. Replication lag and status
            replication_data = execute_query(conn, QUERY_REPLICATION_STATUS)
            if replication_data:
                write_to_csv('replication_lag', replication_data)
                for row in replication_data:
                    lag_mb = row['total_lag_bytes'] / (1024*1024) if row['total_lag_bytes'] else 0
                    print(f"  → Replication Lag: {lag_mb:.2f} MB | State: {row['state']} | App: {row['application_name']}")
            else:
                print("  → No active replication subscribers")

            # 2. WAL statistics
            wal_data = execute_query(conn, QUERY_WAL_STATS)
            wal_data_with_deltas = calculate_wal_deltas(wal_data)
            if wal_data_with_deltas:
                write_to_csv('wal_stats', wal_data_with_deltas)
                for row in wal_data_with_deltas:
                    wal_rate_mb = row.get('wal_mb_per_sec', 0)
                    print(f"  → WAL Generation: {wal_rate_mb:.2f} MB/s")

            # 3. Activity statistics
            activity_data = execute_query(conn, QUERY_ACTIVITY_STATS)
            if activity_data:
                write_to_csv('activity', activity_data)
                for row in activity_data:
                    print(f"  → Connections: {row['total_connections']} total | {row['active_connections']} active | {row['walsender_count']} walsenders")

            # 4. Database statistics
            db_data = execute_query(conn, QUERY_DATABASE_STATS)
            db_data_with_deltas = calculate_db_deltas(db_data)
            if db_data_with_deltas:
                write_to_csv('database', db_data_with_deltas)
                for row in db_data_with_deltas:
                    print(f"  → TPS: {row['tps']:.1f} | Inserts/s: {row['inserts_per_sec']:.1f} | Cache Hit: {row['cache_hit_ratio']:.1f}%")

            # 5. Current WAL LSN
            wal_lsn_data = execute_query(conn, QUERY_WAL_LSN)
            if wal_lsn_data:
                write_to_csv('wal_lsn', wal_lsn_data)

            # 6. Replication slots
            slots_data = execute_query(conn, QUERY_REPLICATION_SLOTS)
            if slots_data:
                write_to_csv('replication_slots', slots_data)
                for row in slots_data:
                    confirmed_lag_mb = row['confirmed_lag_bytes'] / (1024*1024) if row['confirmed_lag_bytes'] else 0
                    status = "ACTIVE" if row['active'] else "INACTIVE"
                    print(f"  → Slot: {row['slot_name']} | {status} | Lag: {confirmed_lag_mb:.2f} MB")

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
            print("✓ Closed publisher connection")

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
