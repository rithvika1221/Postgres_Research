# Transaction Size Experiment Scripts

This directory contains 3 production-quality Python scripts for testing how transaction size (rows per commit) affects PostgreSQL logical replication lag.

## Overview

The experiment tests 6 phases with **increasing concurrency, TPS, AND transaction size**:

| Phase | Users | Target TPS | Rows/Commit | Duration |
|-------|-------|-----------|-------------|----------|
| 1 | 1 | 100 | 1 | 120s |
| 2 | 4 | 500 | 10 | 120s |
| 3 | 8 | 1,000 | 50 | 120s |
| 4 | 16 | 2,500 | 100 | 120s |
| 5 | 32 | 5,000 | 500 | 120s |
| 6 | 64 | 5,000 | 1,000 | 120s |

**Total duration: 12 minutes**. Monitoring should run for 13 minutes (780 seconds).

### Key Design

- **TPS = TRANSACTIONS per second** (not rows)
- Phase 5: 5,000 txns/sec × 500 rows/txn = **2,500,000 rows/sec** theoretical max
- Each worker is rate-limited to `(target_tps / num_workers)` COMMITS per second
- Batch INSERT for efficiency (multiple VALUES in single statement)

## Scripts

### 1. load_generator.py (Publisher VM)

Load generator that orchestrates the 6-phase experiment.

**Features:**
- Multiprocessing with shared phase state
- Token-bucket rate limiter for commit pacing
- Batch INSERT (multiple VALUES) for efficiency
- Graceful worker lifecycle management (spawn → terminate between phases)
- Status file (`tps_phase.txt`) with phase context
- Separate tracking of actual TPS and rows/sec

**Usage:**
```bash
python3 load_generator.py \
  --host localhost \
  --port 5432 \
  --dbname pub \
  --user postgres \
  --password Aarush@123 \
  --output-dir .
```

**Table:** `ingest_data (payload TEXT, big_payload TEXT)`

**Output:** `tps_phase.txt` status file (updated each phase)

### 2. monitor_publisher_xlsx.py (Publisher VM)

Monitors publisher-side metrics and exports to Excel.

**5 Worksheets:**
1. **Replication_Lag**: Slot LSN differences (lag in bytes)
2. **WAL_Stats**: WAL generation and consumption metrics
3. **Database_Stats**: Transactions committed/rolled back, tuple counters
4. **Connections**: Client and replication connection counts
5. **Replication_Slots**: Slot state, restart_lsn, confirmed_flush_lsn

**Features:**
- Time-based phase detection with fallback to `tps_phase.txt`
- Blue header (#2F5496), frozen panes, autofilter
- 13-minute monitoring window (780s)
- Phase context columns: TPS Phase, Target TPS, Workers, Rows/Commit

**Usage:**
```bash
python3 monitor_publisher_xlsx.py \
  --host localhost \
  --port 5432 \
  --dbname pub \
  --user postgres \
  --password Aarush@123 \
  --duration 780 \
  --interval 2 \
  --output-dir .
```

**Output:** `publisher_metrics_txnsize_{timestamp}.xlsx`

### 3. monitor_subscriber_xlsx.py (Subscriber VM)

Monitors subscriber-side metrics and exports to Excel.

**4 Worksheets:**
1. **Subscription_Lag**: Lag between publisher and subscriber (via cross-VM connection)
2. **Apply_Stats**: Apply worker statistics (leader_pid, flush_lsn, replay_lsn)
3. **Worker_Activity**: Individual apply worker processes (pid, state, query_start)
4. **Table_Status**: Subscription table row counts and statistics

**Features:**
- Time-based phase detection only (no local `tps_phase.txt`)
- Green header (#548235), frozen panes, autofilter
- 13-minute monitoring window (780s)
- Cross-VM publisher connection: `172.16.0.4:5432`

**Usage:**
```bash
python3 monitor_subscriber_xlsx.py \
  --subscriber-host localhost \
  --subscriber-port 5432 \
  --subscriber-db sub \
  --subscriber-user postgres \
  --subscriber-password Aarush@123 \
  --publisher-crossvm-host 172.16.0.4 \
  --publisher-crossvm-port 5432 \
  --publisher-db pub \
  --publisher-user postgres \
  --publisher-password Aarush@123 \
  --duration 780 \
  --interval 2 \
  --output-dir .
```

**Output:** `subscriber_metrics_txnsize_{timestamp}.xlsx`

## Database Configuration

All scripts use these hardcoded defaults (can be overridden via arguments):

### Publisher
- Host: `localhost:5432`
- Database: `pub`
- User: `postgres`
- Password: `Aarush@123`
- Table: `ingest_data`

### Subscriber
- Host: `localhost:5432`
- Database: `sub`
- User: `postgres`
- Password: `Aarush@123`

### Publisher (from Subscriber, cross-VM)
- Host: `172.16.0.4:5432`
- Database: `pub`
- User: `postgres`
- Password: `Aarush@123`

## Running the Experiment

### Step 1: Start Load Generator (Publisher VM)
```bash
cd /sessions/loving-serene-pascal/mnt/Postgres_Reaserch/Postgres_Research/scripts/exp_transaction_size
python3 load_generator.py --output-dir ./results
```

### Step 2: Start Publisher Monitor (Publisher VM) in another terminal
```bash
python3 monitor_publisher_xlsx.py --duration 780 --interval 2 --output-dir ./results
```

### Step 3: Start Subscriber Monitor (Subscriber VM) in another terminal
```bash
python3 monitor_subscriber_xlsx.py \
  --subscriber-host localhost \
  --publisher-crossvm-host 172.16.0.4 \
  --duration 780 \
  --interval 2 \
  --output-dir ./results
```

All three should run for approximately 12-13 minutes.

## Output Files

### load_generator.py
- `tps_phase.txt`: Phase status (timestamp, phase name, target TPS, workers, rows/commit)

### monitor_publisher_xlsx.py
- `publisher_metrics_txnsize_YYYYMMDD_HHMMSS.xlsx`: Publisher metrics (5 sheets)

### monitor_subscriber_xlsx.py
- `subscriber_metrics_txnsize_YYYYMMDD_HHMMSS.xlsx`: Subscriber metrics (4 sheets)

## Dependencies

- Python 3.6+
- `psycopg2`: PostgreSQL adapter
- `openpyxl`: Excel workbook creation

Install with:
```bash
pip3 install psycopg2-binary openpyxl
```

## Production Quality Features

All scripts include:
- Comprehensive docstrings
- `argparse` for CLI arguments
- Error handling and graceful degradation
- Progress logging with timestamps
- Proper signal handling (Ctrl+C)
- `py_compile` compatible
- Connection pooling and cleanup
- Worker lifecycle management
- Rate limiting for smooth load

## Notes

- TPS refers to TRANSACTIONS per second, not rows per second
- Row-per-second is calculated as: `TPS × rows_per_commit`
- Phase status file enables phase context tracking across VMs
- Monitors use 780-second duration to capture full 720-second experiment
- Rate limiter uses token bucket for smooth, evenly-paced load
