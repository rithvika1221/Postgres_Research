# Round 3: Measuring PostgreSQL Logical Replication Lag under Controlled Workloads

Data, scripts and analysis for the revised manuscript
"Measuring PostgreSQL Logical Replication Lag under Controlled Workloads: Client Concurrency,
Transaction Batching, Row Size and Inter-Region Network Latency" (Rithvika Devisetti, NHSJS).

Everything in the paper's Results and Appendix A can be regenerated from this folder.

## Layout

```
round3/
  data/       raw output of the 23 runs (88 factor levels), PostgreSQL 18.6
  scripts/    the campaign software that produced the data
  analysis/   the scripts that turn data/ into the tables, statistics and figures in the paper
```

### data/
For each run `<family>_rep<n>` (B_concurrency x3, C_batching x3, D_rowsize x3, E_duration x2,
F_lat_centralus / eastus / northeurope / centralindia x3 each):

- `manifest_<run>.json` – configuration hash, level order and seeds, PostgreSQL settings on both
  machines, per-level achieved rates, pre-/post-drain records and the capacity flag.
- `publisher_<run>.csv` – 1 Hz samples on the publisher: `pg_stat_wal`, `pg_stat_database`,
  `pg_stat_replication` lag columns (bytes and time), CPU/disk/network counters, tagged with run,
  level and phase.
- `subscriber_<run>.csv` – 1 Hz samples on the subscriber (apply rate, CPU, disk, network).
- `*_events.log` – timestamped orchestrator events for the run.
- `01_schema_publisher.sql`, `02_schema_subscriber.sql` – the replicated table, indexes,
  publication and subscription.

Database passwords have been replaced by `********` in the manifests' command lines.

### scripts/
- `loadgen.py` – load generator with the WAL-byte-rate controller (targets `pg_stat_wal.wal_bytes`).
- `orchestrate.py` – runs one family: randomised level order, pre-drain, load, post-drain, manifest.
- `monitor.py` – the 1 Hz sampler used on both machines.
- `run_region.py`, `run_all_regions.py`, `supervisor.py`, `state_relay.py`,
  `start_*.ps1` – drivers for the family F region runs.
- `preflight_campaign.py`, `preflight_region.py`, `verify_setup.py`, `check_*.py`,
  `validate_run.py` – pre-flight and validation checks.
- `matrix_*.json` – the factor levels of each family (Table 1 of the paper).
- `postgresql_settings.md` – the server configuration used on every machine.

### analysis/
See `analysis/README.md`. `python3 build_runlevel.py && python3 stats.py && python3 figures.py`
rebuilds the run-level table (Appendix A), every statistic quoted in the paper (`stats.json`) and
the six figures. Edit the `DATA` path at the top of `build_runlevel.py` to point at `../data`.
