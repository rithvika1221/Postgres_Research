#!/usr/bin/env python3
"""
Publisher Replication Monitor — 1 User, Variable Throughput
Experiment: 1 user (-c 1 -j 1) with TPS stepping every 2 minutes

Monitors PostgreSQL logical replication metrics on the PUBLISHER VM
and writes all data to a single .xlsx file with 5 sheets.

IMPORTANT: Each row includes the current TPS phase so you can correlate
metrics with the target throughput at that point in time.

TPS Schedule (must match load_generator.py):
  Phase 1 (0:00 - 2:00)  →  100 TPS
  Phase 2 (2:00 - 4:00)  →  500 TPS
  Phase 3 (4:00 - 6:00)  → 1000 TPS
  Phase 4 (6:00 - 8:00)  → 2500 TPS
  Phase 5 (8:00 - 10:00) → 5000 TPS

Sheets:
  1. Replication_Lag    — LSN positions, lag bytes, flush/write/replay lag
  2. WAL_Stats          — WAL generation rate, current LSN, delta bytes
  3. Database_Stats     — Transactions, tuple ops, blocks, temp files
  4. Connections        — Active connections by state and wait type
  5. Replication_Slots  — Slot status, retained WAL, active state

Run on: PUBLISHER VM
Requires: psycopg2, openpyxl
Usage:   python3 monitor_publisher_xlsx.py
"""

import os
import sys
import time
import argparse
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    print("ERROR: openpyxl not installed. Run: pip install openpyxl")
    sys.exit(1)


# ─── Configuration ───────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pub",
    "user": "postgres",
    "password": "Aarush@123",
}

SAMPLE_INTERVAL = 5        # seconds between samples
DURATION_SECONDS = 660     # 11 minutes (10-min experiment + 1-min buffer)
OUTPUT_DIR = "."

# TPS schedule — MUST match load_generator.py exactly
TPS_SCHEDULE = [
    ("Phase 1: 100 TPS",   100,  120),
    ("Phase 2: 500 TPS",   500,  120),
    ("Phase 3: 1000 TPS", 1000,  120),
    ("Phase 4: 2500 TPS", 2500,  120),
    ("Phase 5: 5000 TPS", 5000,  120),
]

# Status file written by load_generator.py
STATUS_FILE = "tps_phase.txt"


# ─── TPS Phase Detection ────────────────────────────────────────────────────

def get_tps_phase_from_file():
    """Read the current TPS phase from the status file written by load_generator."""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                lines = f.readlines()
            phase_name = lines[0].strip() if len(lines) > 0 else "Unknown"
            target_tps = 0
            for line in lines:
                if line.startswith("target_tps="):
                    target_tps = int(line.split("=")[1].strip())
            return phase_name, target_tps
    except Exception:
        pass
    return "Unknown", 0


def get_tps_phase_from_time(elapsed_seconds):
    """
    Determine the current TPS phase based on elapsed time.
    Fallback if status file is not available.
    """
    cumulative = 0
    for phase_name, target_tps, duration in TPS_SCHEDULE:
        if elapsed_seconds < cumulative + duration:
            return phase_name, target_tps
        cumulative += duration
    return "Completed", 0


def get_current_tps_phase(elapsed_seconds):
    """
    Get current TPS phase. Prefer status file (live), fall back to time-based.
    """
    phase_name, target_tps = get_tps_phase_from_file()
    if phase_name == "Unknown" or target_tps == 0:
        phase_name, target_tps = get_tps_phase_from_time(elapsed_seconds)
    return phase_name, target_tps


# ─── Excel Styling ───────────────────────────────────────────────────────────
HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)

# Highlight fill for TPS phase columns
TPS_COL_FILL = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")


def style_header_row(ws, num_cols):
    """Apply styling to the header row."""
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = THIN_BORDER
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions


def auto_fit_columns(ws):
    """Auto-fit column widths based on content."""
    for col in ws.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value is not None:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_length + 4, 40)


# ─── SQL Queries ─────────────────────────────────────────────────────────────

QUERY_REPLICATION_LAG = """
SELECT
    now()                                    AS sample_time,
    pid,
    usename,
    application_name,
    client_addr::text,
    state,
    sent_lsn::text,
    write_lsn::text,
    flush_lsn::text,
    replay_lsn::text,
    pg_wal_lsn_diff(sent_lsn, write_lsn)    AS write_lag_bytes,
    pg_wal_lsn_diff(sent_lsn, flush_lsn)    AS flush_lag_bytes,
    pg_wal_lsn_diff(sent_lsn, replay_lsn)   AS replay_lag_bytes,
    write_lag::text,
    flush_lag::text,
    replay_lag::text,
    sync_state
FROM pg_stat_replication;
"""

QUERY_WAL_STATS = """
SELECT
    now()                                     AS sample_time,
    pg_current_wal_lsn()::text                AS current_wal_lsn,
    pg_current_wal_insert_lsn()::text         AS current_wal_insert_lsn,
    pg_wal_lsn_diff(
        pg_current_wal_insert_lsn(),
        pg_current_wal_lsn()
    )                                         AS insert_ahead_bytes,
    (SELECT count(*) FROM pg_ls_waldir())     AS wal_file_count,
    pg_size_pretty(
        sum(size)
    )                                         AS total_wal_size
FROM pg_ls_waldir();
"""

QUERY_DATABASE_STATS = """
SELECT
    now()                    AS sample_time,
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
    blk_write_time
FROM pg_stat_database
WHERE datname = 'pub';
"""

QUERY_CONNECTIONS = """
SELECT
    now()              AS sample_time,
    state,
    wait_event_type,
    wait_event,
    count(*)           AS connection_count,
    string_agg(DISTINCT application_name, ', ') AS applications
FROM pg_stat_activity
WHERE datname = 'pub'
GROUP BY state, wait_event_type, wait_event
ORDER BY count(*) DESC;
"""

QUERY_REPLICATION_SLOTS = """
SELECT
    now()                        AS sample_time,
    slot_name,
    plugin,
    slot_type,
    active,
    active_pid,
    restart_lsn::text,
    confirmed_flush_lsn::text,
    pg_wal_lsn_diff(
        pg_current_wal_lsn(),
        restart_lsn
    )                            AS retained_wal_bytes,
    pg_size_pretty(
        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            restart_lsn
        )
    )                            AS retained_wal_pretty,
    wal_status
FROM pg_replication_slots;
"""


# ─── Sheet Definitions ───────────────────────────────────────────────────────
# NOTE: "TPS Phase" and "Target TPS" are prepended to each sheet as the
# first two columns AFTER Sample Time.

SHEETS = [
    {
        "name": "Replication_Lag",
        "query": QUERY_REPLICATION_LAG,
        "headers": [
            "Sample Time", "TPS Phase", "Target TPS",
            "PID", "User", "Application", "Client Addr",
            "State", "Sent LSN", "Write LSN", "Flush LSN", "Replay LSN",
            "Write Lag (bytes)", "Flush Lag (bytes)", "Replay Lag (bytes)",
            "Write Lag (time)", "Flush Lag (time)", "Replay Lag (time)",
            "Sync State"
        ],
    },
    {
        "name": "WAL_Stats",
        "query": QUERY_WAL_STATS,
        "headers": [
            "Sample Time", "TPS Phase", "Target TPS",
            "Current WAL LSN", "Current WAL Insert LSN",
            "Insert Ahead (bytes)", "WAL File Count", "Total WAL Size"
        ],
    },
    {
        "name": "Database_Stats",
        "query": QUERY_DATABASE_STATS,
        "headers": [
            "Sample Time", "TPS Phase", "Target TPS",
            "Backends", "Commits", "Rollbacks",
            "Blocks Read", "Blocks Hit", "Tuples Returned", "Tuples Fetched",
            "Tuples Inserted", "Tuples Updated", "Tuples Deleted",
            "Conflicts", "Temp Files", "Temp Bytes", "Deadlocks",
            "Block Read Time (ms)", "Block Write Time (ms)"
        ],
    },
    {
        "name": "Connections",
        "query": QUERY_CONNECTIONS,
        "headers": [
            "Sample Time", "TPS Phase", "Target TPS",
            "State", "Wait Event Type", "Wait Event",
            "Connection Count", "Applications"
        ],
    },
    {
        "name": "Replication_Slots",
        "query": QUERY_REPLICATION_SLOTS,
        "headers": [
            "Sample Time", "TPS Phase", "Target TPS",
            "Slot Name", "Plugin", "Slot Type",
            "Active", "Active PID", "Restart LSN", "Confirmed Flush LSN",
            "Retained WAL (bytes)", "Retained WAL (pretty)", "WAL Status"
        ],
    },
]


# ─── Main Monitor ────────────────────────────────────────────────────────────

def create_workbook():
    """Create workbook with all sheet headers."""
    wb = Workbook()
    wb.remove(wb.active)

    for sheet_def in SHEETS:
        ws = wb.create_sheet(title=sheet_def["name"])
        ws.append(sheet_def["headers"])
        style_header_row(ws, len(sheet_def["headers"]))

    return wb


def collect_sample(conn, sheet_data, elapsed_seconds):
    """Run all queries, prepend TPS phase info, append to sheet data."""
    cur = conn.cursor()

    # Get current TPS phase
    phase_name, target_tps = get_current_tps_phase(elapsed_seconds)

    for i, sheet_def in enumerate(SHEETS):
        try:
            cur.execute(sheet_def["query"])
            rows = cur.fetchall()
            for row in rows:
                processed = []
                first = True
                for val in row:
                    if first:
                        # First column is sample_time — add it, then inject TPS cols
                        if isinstance(val, datetime):
                            processed.append(val.strftime("%Y-%m-%d %H:%M:%S"))
                        else:
                            processed.append(str(val))
                        processed.append(phase_name)
                        processed.append(target_tps)
                        first = False
                        continue

                    if val is None:
                        processed.append("")
                    elif isinstance(val, datetime):
                        processed.append(val.strftime("%Y-%m-%d %H:%M:%S"))
                    elif isinstance(val, bool):
                        processed.append("Yes" if val else "No")
                    else:
                        processed.append(val)
                sheet_data[i].append(processed)

            if not rows:
                no_data = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                           phase_name, target_tps, "NO DATA"]
                no_data.extend([""] * (len(sheet_def["headers"]) - 4))
                sheet_data[i].append(no_data)

        except Exception as e:
            error_row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         phase_name, target_tps, f"QUERY ERROR: {e}"]
            error_row.extend([""] * (len(sheet_def["headers"]) - 4))
            sheet_data[i].append(error_row)
            conn.rollback()

    conn.commit()
    return phase_name, target_tps


def save_workbook(wb, sheet_data, output_path):
    """Write collected data to workbook and save."""
    for i, sheet_def in enumerate(SHEETS):
        ws = wb[sheet_def["name"]]
        for row in sheet_data[i]:
            ws.append(row)
        auto_fit_columns(ws)
        if ws.max_row > 1:
            ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"

    wb.save(output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Publisher monitor for 1-user variable TPS experiment"
    )
    parser.add_argument(
        "--duration", type=int, default=DURATION_SECONDS,
        help=f"Monitoring duration in seconds (default: {DURATION_SECONDS})"
    )
    parser.add_argument(
        "--interval", type=int, default=SAMPLE_INTERVAL,
        help=f"Sample interval in seconds (default: {SAMPLE_INTERVAL})"
    )
    parser.add_argument(
        "--output-dir", type=str, default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})"
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
    duration = args.duration
    interval = args.interval

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"publisher_metrics_1user_varTPS_{timestamp}.xlsx"
    output_path = os.path.join(args.output_dir, output_filename)

    print("=" * 65)
    print("  Publisher Replication Monitor — Variable TPS")
    print("  Experiment: 1 user (-c 1 -j 1), TPS stepping every 2 min")
    print("=" * 65)
    print(f"  Host:       {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Database:   {DB_CONFIG['dbname']}")
    print(f"  Interval:   {interval}s")
    print(f"  Duration:   {duration}s ({duration // 60} min)")
    print(f"  Output:     {output_path}")
    print(f"  Sheets:     {', '.join(s['name'] for s in SHEETS)}")
    print(f"\n  TPS Schedule (each row logged with current phase):")
    cumulative = 0
    for name, tps, dur in TPS_SCHEDULE:
        m1, s1 = divmod(cumulative, 60)
        m2, s2 = divmod(cumulative + dur, 60)
        print(f"    {m1}:{s1:02d} - {m2}:{s2:02d}  →  {tps:>5,} TPS")
        cumulative += dur
    print("=" * 65)

    # Connect
    print("\n[1/2] Connecting to publisher database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print("  Connected successfully.")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # Create workbook
    print("[2/2] Creating workbook with 5 sheets...")
    wb = create_workbook()
    sheet_data = [[] for _ in SHEETS]

    # Monitor loop
    print(f"\n  Monitoring started at {datetime.now().strftime('%H:%M:%S')}...\n")

    start_time = time.time()
    end_time = start_time + duration
    sample_count = 0

    try:
        while time.time() < end_time:
            sample_start = time.time()
            elapsed = time.time() - start_time

            phase_name, target_tps = collect_sample(conn, sheet_data, elapsed)
            sample_count += 1

            # Quick lag summary
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT pg_wal_lsn_diff(sent_lsn, replay_lsn)
                    FROM pg_stat_replication LIMIT 1
                """)
                result = cur.fetchone()
                lag_str = f"{result[0]:,} bytes" if result else "No subscribers"
                conn.commit()
            except Exception:
                lag_str = "N/A"
                conn.rollback()

            elapsed_int = int(elapsed)
            remaining = max(0, int(end_time - time.time()))
            print(
                f"  Sample {sample_count:>4} | "
                f"{elapsed_int:>4}s | "
                f"{phase_name:25s} | "
                f"Lag: {lag_str:>15s} | "
                f"Remaining: {remaining}s"
            )

            # Wait for next interval
            elapsed_sample = time.time() - sample_start
            sleep_time = max(0, interval - elapsed_sample)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n  Ctrl+C — stopping monitor...")

    finally:
        print(f"\n  Saving {output_filename}...")
        save_workbook(wb, sheet_data, output_path)
        conn.close()

        total_rows = sum(len(sd) for sd in sheet_data)
        print("\n" + "=" * 65)
        print("  MONITORING COMPLETE")
        print("=" * 65)
        print(f"  Samples:    {sample_count}")
        print(f"  Total rows: {total_rows} across 5 sheets")
        print(f"  Output:     {output_path}")
        print(f"  File size:  {os.path.getsize(output_path):,} bytes")
        print("=" * 65)


if __name__ == "__main__":
    main()
