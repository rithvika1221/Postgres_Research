#!/usr/bin/env python3
"""
Comprehensive Logical Replication Monitor
Collects metrics from both Publisher and Subscriber PostgreSQL instances
Outputs to CSV files for analysis in Excel

Usage:
    export PGPASSWORD='your_password'
    python replication_monitor.py

Output Files:
    - replication_lag.csv: Replication lag and LSN positions
    - wal_stats.csv: WAL generation statistics (publisher)
    - activity_stats.csv: Connection and query activity
    - database_stats.csv: Transaction rates and database statistics
    - subscription_stats.csv: Subscription lag (subscriber)
"""

import os
import sys
import time
import csv
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor

# =============================================================================
# CONFIGURATION
# =============================================================================

# Publisher (Primary) Configuration
PUBLISHER_CONFIG = {
    'host': 'pg-source2-westus2.postgres.database.azure.com',
    'port': 5432,
    'dbname': 'pub',
    'user': 'postgres',
    'password': os.getenv('PGPASSWORD'),
    'sslmode': 'require'
}

# Subscriber (Replica) Configuration
SUBSCRIBER_CONFIG = {
    'host': 'pg-replica2-westus2.postgres.database.azure.com',  # UPDATE THIS
    'port': 5432,
    'dbname': 'sub',  # UPDATE THIS if different
    'user': 'postgres',
    'password': os.getenv('PGPASSWORD'),
    'sslmode': 'require'
}

# Monitoring Configuration
SAMPLE_INTERVAL = 5  # seconds between samples
OUTPUT_DIR = '.'     # directory for CSV files

# =============================================================================
# SQL QUERIES
# =============================================================================

# Publisher: Replication lag and LSN positions
QUERY_REPLICATION_LAG = """
SELECT
    NOW() as timestamp,
    application_name,
    client_addr,
    state,
    sync_state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    pg_wal_lsn_diff(sent_lsn, replay_lsn) as lag_bytes,
    pg_wal_lsn_diff(sent_lsn, write_lsn) as write_lag_bytes,
    pg_wal_lsn_diff(write_lsn, flush_lsn) as flush_lag_bytes,
    pg_wal_lsn_diff(flush_lsn, replay_lsn) as replay_lag_bytes,
    backend_start,
    backend_xmin,
    reply_time
FROM pg_stat_replication;
"""

# Publisher: WAL generation statistics
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

# Publisher: Activity and connections
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
    COUNT(*) FILTER (WHERE backend_type = 'walsender') as walsender_count
FROM pg_stat_activity;
"""

# Publisher: Database statistics
QUERY_DATABASE_STATS = """
SELECT
    NOW() as timestamp,
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
    blk_read_time,
    blk_write_time,
    stats_reset
FROM pg_stat_database
WHERE datname = current_database();
"""

# Subscriber: Subscription lag and status
QUERY_SUBSCRIPTION_STATS = """
SELECT
    NOW() as timestamp,
    subname,
    pid,
    relid,
    received_lsn,
    last_msg_send_time,
    last_msg_receipt_time,
    latest_end_lsn,
    latest_end_time,
    pg_wal_lsn_diff(latest_end_lsn, received_lsn) as subscriber_lag_bytes
FROM pg_stat_subscription;
"""

# Publisher: Current WAL LSN (for lag calculation)
QUERY_CURRENT_WAL_LSN = """
SELECT
    NOW() as timestamp,
    pg_current_wal_lsn() as current_wal_lsn,
    pg_current_wal_insert_lsn() as current_wal_insert_lsn
"""

# =============================================================================
# CSV FILE SETUP
# =============================================================================

CSV_FILES = {
    'replication_lag': {
        'filename': f'{OUTPUT_DIR}/replication_lag.csv',
        'fieldnames': ['timestamp', 'application_name', 'client_addr', 'state',
                      'sync_state', 'sent_lsn', 'write_lsn', 'flush_lsn',
                      'replay_lsn', 'lag_bytes', 'write_lag_bytes',
                      'flush_lag_bytes', 'replay_lag_bytes', 'backend_start',
                      'backend_xmin', 'reply_time']
    },
    'wal_stats': {
        'filename': f'{OUTPUT_DIR}/wal_stats.csv',
        'fieldnames': ['timestamp', 'wal_records', 'wal_fpi', 'wal_bytes',
                      'wal_buffers_full', 'wal_write', 'wal_sync',
                      'wal_write_time', 'wal_sync_time', 'stats_reset',
                      'wal_records_delta', 'wal_bytes_delta', 'wal_bytes_per_sec']
    },
    'activity_stats': {
        'filename': f'{OUTPUT_DIR}/activity_stats.csv',
        'fieldnames': ['timestamp', 'total_connections', 'active_connections',
                      'idle_connections', 'idle_in_transaction', 'waiting_connections',
                      'io_waiting', 'lock_waiting', 'walsender_count']
    },
    'database_stats': {
        'filename': f'{OUTPUT_DIR}/database_stats.csv',
        'fieldnames': ['timestamp', 'numbackends', 'xact_commit', 'xact_rollback',
                      'blks_read', 'blks_hit', 'tup_returned', 'tup_fetched',
                      'tup_inserted', 'tup_updated', 'tup_deleted', 'conflicts',
                      'temp_files', 'temp_bytes', 'deadlocks', 'blk_read_time',
                      'blk_write_time', 'stats_reset', 'tps', 'cache_hit_ratio',
                      'inserts_per_sec', 'updates_per_sec', 'deletes_per_sec']
    },
    'subscription_stats': {
        'filename': f'{OUTPUT_DIR}/subscription_stats.csv',
        'fieldnames': ['timestamp', 'subname', 'pid', 'relid', 'received_lsn',
                      'last_msg_send_time', 'last_msg_receipt_time',
                      'latest_end_lsn', 'latest_end_time', 'subscriber_lag_bytes']
    },
    'wal_lsn': {
        'filename': f'{OUTPUT_DIR}/wal_lsn.csv',
        'fieldnames': ['timestamp', 'current_wal_lsn', 'current_wal_insert_lsn']
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

def connect_db(config, db_type):
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**config)
        print(f"✓ Connected to {db_type}: {config['host']}/{config['dbname']}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to {db_type}: {e}")
        return None

def execute_query(conn, query, db_type=""):
    """Execute query and return results as list of dicts"""
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            # Convert to list of dicts
            return [dict(row) for row in results]
    except Exception as e:
        print(f"✗ Query error on {db_type}: {e}")
        return []

# =============================================================================
# METRIC CALCULATION
# =============================================================================

# Previous values for delta calculations
prev_wal_stats = None
prev_db_stats = None
prev_timestamp = None

def calculate_wal_deltas(current_stats):
    """Calculate WAL generation rate"""
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
            else:
                row_copy['wal_records_delta'] = 0
                row_copy['wal_bytes_delta'] = 0
                row_copy['wal_bytes_per_sec'] = 0
        else:
            row_copy['wal_records_delta'] = 0
            row_copy['wal_bytes_delta'] = 0
            row_copy['wal_bytes_per_sec'] = 0

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
    if not PUBLISHER_CONFIG['password']:
        print("✗ Error: PGPASSWORD environment variable not set")
        print("  Run: export PGPASSWORD='your_password'")
        sys.exit(1)

    print("\n" + "="*70)
    print("PostgreSQL Logical Replication Monitor")
    print("="*70)
    print(f"Sample interval: {SAMPLE_INTERVAL} seconds")
    print(f"Output directory: {OUTPUT_DIR}")
    print("Press Ctrl+C to stop\n")

    # Initialize CSV files
    init_csv_files()

    # Connect to databases
    pub_conn = connect_db(PUBLISHER_CONFIG, "Publisher")
    sub_conn = connect_db(SUBSCRIBER_CONFIG, "Subscriber")

    if not pub_conn:
        print("\n✗ Cannot continue without publisher connection")
        sys.exit(1)

    if not sub_conn:
        print("⚠ Warning: Subscriber connection failed. Will only monitor publisher.\n")

    print("\n" + "="*70)
    print("Starting monitoring... (Ctrl+C to stop)")
    print("="*70 + "\n")

    sample_count = 0

    try:
        while True:
            sample_count += 1
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            print(f"[{timestamp}] Sample #{sample_count}")

            # ==== PUBLISHER METRICS ====

            # 1. Replication lag
            replication_data = execute_query(pub_conn, QUERY_REPLICATION_LAG, "Publisher")
            if replication_data:
                write_to_csv('replication_lag', replication_data)
                for row in replication_data:
                    lag_mb = row['lag_bytes'] / (1024*1024) if row['lag_bytes'] else 0
                    print(f"  Replication Lag: {lag_mb:.2f} MB ({row['application_name']})")

            # 2. WAL statistics
            wal_data = execute_query(pub_conn, QUERY_WAL_STATS, "Publisher")
            wal_data_with_deltas = calculate_wal_deltas(wal_data)
            if wal_data_with_deltas:
                write_to_csv('wal_stats', wal_data_with_deltas)
                for row in wal_data_with_deltas:
                    wal_rate_mb = row['wal_bytes_per_sec'] / (1024*1024) if 'wal_bytes_per_sec' in row else 0
                    print(f"  WAL Generation: {wal_rate_mb:.2f} MB/s")

            # 3. Activity statistics
            activity_data = execute_query(pub_conn, QUERY_ACTIVITY_STATS, "Publisher")
            if activity_data:
                write_to_csv('activity_stats', activity_data)
                for row in activity_data:
                    print(f"  Connections: {row['total_connections']} total, {row['active_connections']} active")

            # 4. Database statistics
            db_data = execute_query(pub_conn, QUERY_DATABASE_STATS, "Publisher")
            db_data_with_deltas = calculate_db_deltas(db_data)
            if db_data_with_deltas:
                write_to_csv('database_stats', db_data_with_deltas)
                for row in db_data_with_deltas:
                    print(f"  TPS: {row['tps']:.1f}, Inserts/s: {row['inserts_per_sec']:.1f}")

            # 5. Current WAL LSN
            wal_lsn_data = execute_query(pub_conn, QUERY_CURRENT_WAL_LSN, "Publisher")
            if wal_lsn_data:
                write_to_csv('wal_lsn', wal_lsn_data)

            # ==== SUBSCRIBER METRICS ====

            if sub_conn:
                # 6. Subscription statistics
                sub_data = execute_query(sub_conn, QUERY_SUBSCRIPTION_STATS, "Subscriber")
                if sub_data:
                    write_to_csv('subscription_stats', sub_data)
                    for row in sub_data:
                        sub_lag_mb = row['subscriber_lag_bytes'] / (1024*1024) if row['subscriber_lag_bytes'] else 0
                        print(f"  Subscriber Lag: {sub_lag_mb:.2f} MB ({row['subname']})")

            print()  # Blank line between samples

            # Wait for next sample
            time.sleep(SAMPLE_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        print("Monitoring stopped by user")
        print("="*70)

    finally:
        # Close connections
        if pub_conn:
            pub_conn.close()
            print("✓ Closed publisher connection")
        if sub_conn:
            sub_conn.close()
            print("✓ Closed subscriber connection")

        print(f"\n✓ Collected {sample_count} samples")
        print("\nOutput files:")
        for key, config in CSV_FILES.items():
            print(f"  - {config['filename']}")
        print("\nYou can now import these CSV files into Excel for analysis.\n")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    monitor()
