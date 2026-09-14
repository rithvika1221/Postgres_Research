# Round 3 analysis package — "Measuring PostgreSQL Logical Replication Lag under Controlled Workloads"

This folder is the analysis half of the paper's data-availability statement. Push it to the
`Postgres_Research` repository (for example under `round3/analysis/`) together with the raw run
data (`manifest_*.json`, `publisher_*.csv`, `subscriber_*.csv` for all 23 runs) and the campaign
scripts (`orchestrate.py`, `loadgen.py`, `monitor.py`, `run_region.py`, matrix JSON files).

Contents

- `build_runlevel.py` — reads the raw manifests and 1 Hz monitor CSVs and writes `runlevel.csv`,
  one row per run x level (warm-up of 60 s excluded), with the achieved workload and lag summaries.
- `stats.py` — computes every statistic quoted in the paper from `runlevel.csv` and writes
  `stats.json` (Jonckheere–Terpstra and Page trend tests with permutation p-values, TOST
  equivalence tests, the RTT regression with confidence intervals and leave-one-region-out check,
  set-point and stationarity checks).
- `figures.py` — draws Figures 1–6 of the paper from `runlevel_ext.csv` and `stats.json`.
- `runlevel.csv`, `runlevel_ext.csv` — the run-level table (Appendix A of the paper is a subset of
  the columns).
- `level_summary.csv` — per-level summary across replicates.
- `stats.json` — all computed statistics.
- `figs/` — the six figures at 300 dpi.

Reproduce: `python3 build_runlevel.py && python3 stats.py && python3 figures.py`
(pandas, numpy, scipy, matplotlib). Edit the `DATA` path at the top of `build_runlevel.py` to point
at the folder containing the raw run files.
