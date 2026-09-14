# Round 3 — PostgreSQL 18 configuration

**Single version. PostgreSQL 18 only.** Round 3 does not compare versions;
`pg_version` is not a factor, not a model feature, and not a column in any
analysis. Round 1 and Round 2 data are superseded and are not pooled with
Round 3.

The reviewer's Methods complaint was that only `wal_level = logical` was ever
reported. Set everything below explicitly, and let `capture_environment.py`
record the result — it dumps every non-default setting into the run folder, so
the manuscript can cite exact values rather than "defaults".

## Publisher — append to `postgresql.conf`

```conf
# --- replication ---------------------------------------------------------
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 8GB               # slot retention headroom for above-knee runs
wal_sender_timeout = 60s

# --- write path ----------------------------------------------------------
synchronous_commit = off          # the subject is apply lag, not commit
                                  # durability. State this choice in Methods.
max_wal_size = 16GB               # avoid checkpoint storms inside a level
min_wal_size = 2GB
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
wal_compression = off             # ON makes payload size meaningless

# --- memory --------------------------------------------------------------
shared_buffers = 8GB              # 25% of 32 GB
effective_cache_size = 24GB
work_mem = 64MB
maintenance_work_mem = 2GB

# --- connections ---------------------------------------------------------
max_connections = 200

# --- statistics (the monitor depends on these) ---------------------------
track_counts = on
track_io_timing = on              # blk_read_time / blk_write_time
track_wal_io_timing = on          # WAL write/fsync timing. In PostgreSQL 18
                                  # this feeds pg_stat_io (object='wal'), NOT
                                  # pg_stat_wal: 18 removed wal_write_time and
                                  # wal_sync_time from that view. The monitor
                                  # picks the right source from the server
                                  # version; do not "fix" it back.

# --- logging -------------------------------------------------------------
log_checkpoints = on              # a checkpoint inside a level must be visible
logging_collector = on
log_min_duration_statement = -1
```

## Subscriber — append to `postgresql.conf`

```conf
wal_level = logical
max_replication_slots = 10
max_logical_replication_workers = 4
max_worker_processes = 16
max_sync_workers_per_subscription = 2
max_parallel_apply_workers_per_subscription = 2

synchronous_commit = off
max_wal_size = 16GB
min_wal_size = 2GB
checkpoint_timeout = 15min
shared_buffers = 8GB
effective_cache_size = 24GB
work_mem = 64MB
maintenance_work_mem = 2GB
max_connections = 200
track_counts = on
track_io_timing = on
track_wal_io_timing = on
log_checkpoints = on
logging_collector = on
```

## Both — `pg_hba.conf`

```
host    pub      repl_user    10.0.0.0/16    scram-sha-256
host    all      postgres     127.0.0.1/32   scram-sha-256
```

## Record it

`capture_environment.py` does this automatically and should be run once per host
before the first experiment:

```powershell
py capture_environment.py --role publisher
py capture_environment.py --role subscriber
```

It writes `environment_<role>_<timestamp>.json` containing the exact minor
version, every non-default setting, the full column and index inventory,
subscription parameters, CPU/RAM/disk layout, and the `w32tm` clock offset —
which is the number that proves the negative lag-time samples from Round 2 are
fixed.
