#!/usr/bin/env python3
"""Build the run-level analysis table (one row per run x level) from Round 3 raw data.

Unit of analysis = run (replicate). For each level within a run we take the load window,
discard the first WARMUP_SEC seconds, and summarise the 1 Hz publisher samples.
"""
import glob, json, os, sys
import numpy as np, pandas as pd

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUT = '/home/claude/paper/analysis'
WARMUP_SEC = 60

rows = []
for mf in sorted(glob.glob(os.path.join(DATA, 'manifest_*.json'))):
    m = json.load(open(mf))
    run_id = m['run_id']; fam = m['family']
    pub = pd.read_csv(os.path.join(DATA, f'publisher_{run_id}.csv'))
    subf = os.path.join(DATA, f'subscriber_{run_id}.csv')
    sub = pd.read_csv(subf) if os.path.exists(subf) else None
    cc = m['config_contents']
    rtt = cc['levels'][0].get('network_latency_ms', 0)
    if fam.startswith('F_lat'):
        rtt = cc.get('rtt_provenance', {}).get('measured_median_ms', rtt)
    region = fam.split('_')[-1] if fam.startswith('F_lat') else 'centralus'
    for r in m['results']:
        lid = r['level_id']; p = r['params']
        d = pub[(pub.level_id == lid) & (pub.phase_state == 'load')].copy()
        d['t'] = d.epoch - d.epoch.min()
        w = d[d.t >= WARMUP_SEC]
        lag_ms = w.replay_lag_sec.dropna() * 1000
        wal_first, wal_last = d.wal_bytes.iloc[0], d.wal_bytes.iloc[-1]
        xc_first, xc_last = d.xact_commit.iloc[0], d.xact_commit.iloc[-1]
        ti_first, ti_last = d.tup_inserted.iloc[0], d.tup_inserted.iloc[-1]
        span = d.epoch.iloc[-1] - d.epoch.iloc[0]
        rec = dict(
            family=fam, run_id=run_id, repeat=m['repeat'], region=region, level_id=lid, label=r['label'],
            order_index=r['order_index'], clients=p['clients'], rows_per_commit=p['rows_per_commit'],
            row_bytes=p['row_bytes'], target_wal_mbps=p['target_wal_mbps'], rtt_ms=rtt,
            duration_sec=p['duration_sec'], n_samples=len(w),
            lag_median_ms=lag_ms.median(), lag_p95_ms=np.percentile(lag_ms, 95), lag_mean_ms=lag_ms.mean(),
            lag_p05_ms=np.percentile(lag_ms, 5), lag_max_ms=lag_ms.max(),
            write_lag_median_ms=w.write_lag_sec.median() * 1000, flush_lag_median_ms=w.flush_lag_sec.median() * 1000,
            lag_bytes_median=w.replay_lag_bytes.median(), lag_bytes_p95=np.percentile(w.replay_lag_bytes.dropna(), 95),
            wal_mbps_mean=w.wal_mb_per_sec.mean(), wal_mbps_sd=w.wal_mb_per_sec.std(),
            wal_mbps_level=(wal_last - wal_first) / span / 1e6,
            commits_per_sec=(xc_last - xc_first) / span, rows_per_sec=(ti_last - ti_first) / span,
            wal_bytes_delta=int(wal_last - wal_first), xact_commit_delta=int(xc_last - xc_first), tup_inserted_delta=int(ti_last - ti_first),
            wal_bytes_per_row=(wal_last - wal_first) / max(1, ti_last - ti_first),
            manifest_measured_wal_mbps=r['measured_wal_mb_per_sec'],
            loadgen_commits=r['loadgen']['completed_commits'], loadgen_rows=r['loadgen']['completed_rows_inserted'],
            loadgen_commits_per_sec=r['loadgen']['achieved_commits_per_sec'], loadgen_rows_per_sec=r['loadgen']['achieved_rows_per_sec'],
            sql_errors=r['loadgen']['sql_errors'], setpoint_error_pct=r['setpoint_error_pct'],
            exceeded_capacity=r['exceeded_capacity'], pre_drained=r['pre_drain']['drained'], post_drained=r['post_drain']['drained'],
            pre_start_backlog=r['pre_drain']['start_backlog_bytes'], post_drain_sec=r['post_drain']['drain_sec'],
            lag_growth_bytes_per_sec=r['lag_growth_bytes_per_sec'], peak_lag_bytes=r['peak_lag_during_load_bytes'],
            pub_cpu_mean=w.cpu_pct.mean(), load_seconds=r['load_seconds'],
        )
        # first-minute vs rest, to justify warm-up and steady state
        e = d[d.t < WARMUP_SEC].replay_lag_sec.dropna() * 1000
        rec['lag_median_first60_ms'] = e.median()
        # thirds of the post-warmup window (stationarity check)
        thirds = np.array_split(lag_ms.values, 3)
        rec['lag_median_third1_ms'], rec['lag_median_third2_ms'], rec['lag_median_third3_ms'] = [np.median(x) for x in thirds]
        if sub is not None:
            s = sub[(sub.level_id == lid) & (sub.phase_state == 'load')].copy()
            s['t'] = s.epoch - s.epoch.min(); s = s[s.t >= WARMUP_SEC]
            rec['sub_rows_applied_per_sec'] = s.rows_applied_per_sec.mean()
            rec['sub_commits_per_sec'] = s.commits_per_sec.mean()
            rec['sub_cpu_mean'] = s.cpu_pct.mean()
            rec['sub_disk_write_mbps'] = s.disk_write_bytes_per_sec.mean() / 1e6
        rows.append(rec)

df = pd.DataFrame(rows).sort_values(['family', 'repeat', 'level_id'])
df.to_csv(os.path.join(OUT, 'runlevel.csv'), index=False)
print(df.groupby('family').size())
print(df[['family','run_id','level_id','lag_median_ms','lag_p95_ms','wal_mbps_level','commits_per_sec','rows_per_sec','sub_rows_applied_per_sec','sub_cpu_mean','pub_cpu_mean']].to_string())
