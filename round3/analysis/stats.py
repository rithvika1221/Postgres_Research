#!/usr/bin/env python3
"""Run-level inferential statistics for the Round 3 manuscript.

Unit of analysis is the run (replicate). Each run contributes one summary value (median of the 1 Hz
replay-lag samples after a 60 s warm-up) per factor level.
  * Trend across ordered levels: Jonckheere–Terpstra (JT) statistic with an exact/Monte-Carlo
    permutation p-value, plus Page's L for the blocked (run-as-block) design with within-run
    permutation p-value.
  * Equivalence (family B, 1 vs 64 clients): TOST on paired run-level differences with a
    pre-specified margin of +/-5 ms.
  * Family F: OLS of run-level median lag on measured RTT, with 95 % CI, leave-one-region-out check.
"""
import itertools, json, math, os, random
import numpy as np, pandas as pd
from scipy import stats

OUT = '/home/claude/paper/analysis'
df = pd.read_csv(os.path.join(OUT, 'runlevel.csv'))
rng = random.Random(20260914)
res = {}

def jt_stat(groups):
    """Jonckheere–Terpstra statistic for ordered groups (list of arrays)."""
    J = 0.0
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            for a in groups[i]:
                for b in groups[j]:
                    J += 1.0 if b > a else (0.5 if b == a else 0.0)
    return J

def jt_test(groups, n_perm=200000, alternative='increasing'):
    """Permutation JT test. Returns statistic, expected value, z (normal approx), one-sided and
    two-sided Monte-Carlo p-values."""
    J = jt_stat(groups)
    pooled = np.concatenate(groups); sizes = [len(g) for g in groups]
    N = len(pooled)
    EJ = (N * N - sum(n * n for n in sizes)) / 4.0
    VJ = (N * N * (2 * N + 3) - sum(n * n * (2 * n + 3) for n in sizes)) / 72.0
    z = (J - EJ) / math.sqrt(VJ)
    # Monte-Carlo permutation of the pooled values across groups (ignores blocking)
    cnt_ge = cnt_le = 0
    pooled = list(pooled)
    for _ in range(n_perm):
        rng.shuffle(pooled)
        k = 0; gs = []
        for n in sizes:
            gs.append(pooled[k:k + n]); k += n
        Jp = jt_stat(gs)
        if Jp >= J: cnt_ge += 1
        if Jp <= J: cnt_le += 1
    p_inc = (cnt_ge + 1) / (n_perm + 1); p_dec = (cnt_le + 1) / (n_perm + 1)
    return dict(J=J, EJ=EJ, z=z, p_increasing=p_inc, p_decreasing=p_dec, p_two_sided=min(1.0, 2 * min(p_inc, p_dec)))

def page_test(block_matrix, n_perm=200000):
    """Page's L for ordered alternatives in randomized blocks. block_matrix: runs x levels.
    Within-block ranks; L = sum_j j * R_j. Exact within-block permutation p-value (Monte-Carlo)."""
    M = np.asarray(block_matrix, float); b, k = M.shape
    ranks = np.apply_along_axis(stats.rankdata, 1, M)
    L = float(sum((j + 1) * ranks[:, j].sum() for j in range(k)))
    EL = b * k * (k + 1) ** 2 / 4.0
    VL = b * k * k * (k + 1) * (k * k - 1) / 144.0
    z = (L - EL) / math.sqrt(VL)
    cnt_ge = cnt_le = 0
    cols = list(range(k))
    for _ in range(n_perm):
        Lp = 0.0
        for i in range(b):
            perm = cols[:]; rng.shuffle(perm)
            Lp += sum((j + 1) * ranks[i, perm[j]] for j in range(k))
        if Lp >= L: cnt_ge += 1
        if Lp <= L: cnt_le += 1
    p_inc = (cnt_ge + 1) / (n_perm + 1); p_dec = (cnt_le + 1) / (n_perm + 1)
    return dict(L=L, EL=EL, z=z, p_increasing=p_inc, p_decreasing=p_dec, p_two_sided=min(1.0, 2 * min(p_inc, p_dec)))

def level_matrix(fam, levels, col='lag_median_ms'):
    d = df[df.family == fam]
    runs = sorted(d.run_id.unique())
    M = np.array([[d[(d.run_id == r) & (d.level_id == l)][col].iloc[0] for l in levels] for r in runs])
    return runs, M

def tost_paired(diffs, margin):
    d = np.asarray(diffs, float); n = len(d); m = d.mean(); s = d.std(ddof=1); se = s / math.sqrt(n)
    t_low = (m + margin) / se; t_up = (m - margin) / se
    p_low = 1 - stats.t.cdf(t_low, n - 1)      # H0: mean <= -margin
    p_up = stats.t.cdf(t_up, n - 1)            # H0: mean >= +margin
    ci90 = stats.t.interval(0.90, n - 1, loc=m, scale=se)
    ci95 = stats.t.interval(0.95, n - 1, loc=m, scale=se)
    return dict(n=n, mean_diff=m, sd_diff=s, se=se, t_lower=t_low, t_upper=t_up, p_lower=p_low, p_upper=p_up,
                p_tost=max(p_low, p_up), ci90=list(ci90), ci95=list(ci95), margin=margin, equivalent=max(p_low, p_up) < 0.05)

# ---------------- Family B: concurrency ----------------
B8 = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06']; B30 = ['B07', 'B08', 'B09', 'B10']
resB = {}
for name, levels in [('8MiB', B8), ('30MiB', B30)]:
    runs, M = level_matrix('B_concurrency', levels)
    groups = [M[:, j] for j in range(M.shape[1])]
    r = dict(levels=levels, clients=[int(df[df.level_id == l].clients.iloc[0]) for l in levels],
             run_medians=M.tolist(), level_median=np.median(M, 0).tolist(), level_min=M.min(0).tolist(), level_max=M.max(0).tolist(),
             jt=jt_test(groups), page=page_test(M),
             tost_first_vs_last=tost_paired(M[:, -1] - M[:, 0], 5.0))
    # p95 too
    _, P = level_matrix('B_concurrency', levels, 'lag_p95_ms')
    r['p95_level_median'] = np.median(P, 0).tolist(); r['p95_tost_first_vs_last'] = tost_paired(P[:, -1] - P[:, 0], 5.0)
    _, C = level_matrix('B_concurrency', levels, 'pub_cpu_mean'); r['pub_cpu_level_mean'] = C.mean(0).tolist()
    _, S = level_matrix('B_concurrency', levels, 'sub_cpu_mean'); r['sub_cpu_level_mean'] = S.mean(0).tolist()
    resB[name] = r
res['B'] = resB

# ---------------- Family C: rows per commit at 16 MiB/s ----------------
CL = ['C01', 'C02', 'C03', 'C04', 'C05', 'C06']
runs, M = level_matrix('C_batching', CL); _, P = level_matrix('C_batching', CL, 'lag_p95_ms')
res['C'] = dict(levels=CL, rows_per_commit=[int(df[df.level_id == l].rows_per_commit.iloc[0]) for l in CL],
                run_medians=M.tolist(), level_median=np.median(M, 0).tolist(), level_min=M.min(0).tolist(), level_max=M.max(0).tolist(),
                jt=jt_test([M[:, j] for j in range(6)]), page=page_test(M),
                p95_run=P.tolist(), p95_level_median=np.median(P, 0).tolist(), jt_p95=jt_test([P[:, j] for j in range(6)]), page_p95=page_test(P),
                first_vs_last_diff=(M[:, -1] - M[:, 0]).tolist())
# Spearman between rows/commit and run-level median (18 points) for completeness
d = df[df.family == 'C_batching']
res['C']['spearman_rows_per_commit_vs_median'] = list(stats.spearmanr(np.log10(d.rows_per_commit), d.lag_median_ms))

# ---------------- Family D: row size at 6 MiB/s ----------------
DL = ['D01', 'D02', 'D03', 'D04', 'D05', 'D06']
runs, M = level_matrix('D_rowsize', DL); _, P = level_matrix('D_rowsize', DL, 'lag_p95_ms')
res['D'] = dict(levels=DL, row_bytes=[int(df[df.level_id == l].row_bytes.iloc[0]) for l in DL],
                run_medians=M.tolist(), level_median=np.median(M, 0).tolist(), level_min=M.min(0).tolist(), level_max=M.max(0).tolist(),
                jt=jt_test([M[:, j] for j in range(6)]), page=page_test(M),
                p95_level_median=np.median(P, 0).tolist(), jt_p95=jt_test([P[:, j] for j in range(6)]), page_p95=page_test(P))
d = df[df.family == 'D_rowsize']
res['D']['spearman_row_bytes_vs_median'] = list(stats.spearmanr(np.log10(d.row_bytes), d.lag_median_ms))
res['D']['spearman_commits_per_sec_vs_median'] = list(stats.spearmanr(d.loadgen_commits_per_sec, d.lag_median_ms))

# ---------------- Family E: duration ----------------
EL = ['E01', 'E02', 'E03', 'E04', 'E05']
runs, M = level_matrix('E_duration', EL); _, P = level_matrix('E_duration', EL, 'lag_p95_ms')
res['E'] = dict(levels=EL, duration_min=[int(df[df.level_id == l].duration_sec.iloc[0]) // 60 for l in EL],
                run_medians=M.tolist(), level_median=np.median(M, 0).tolist(), p95_run=P.tolist(),
                page=page_test(M), jt=jt_test([M[:, j] for j in range(5)]), range_ms=float(M.max() - M.min()))
# within-level stationarity across all families: thirds
t = df[['lag_median_third1_ms', 'lag_median_third2_ms', 'lag_median_third3_ms']].values
res['stationarity'] = dict(max_abs_third_diff_ms=float(np.abs(t[:, 2] - t[:, 0]).max()),
                           median_abs_third_diff_ms=float(np.median(np.abs(t[:, 2] - t[:, 0]))),
                           n_levels=int(len(t)),
                           first60_vs_rest_median_abs_diff_ms=float(np.median(np.abs(df.lag_median_first60_ms - df.lag_median_ms))),
                           first60_vs_rest_max_abs_diff_ms=float(np.abs(df.lag_median_first60_ms - df.lag_median_ms).max()))
# order effects: Spearman of order_index vs residual from level median (randomised families)
oe = {}
for fam in ['B_concurrency', 'C_batching', 'D_rowsize']:
    d = df[df.family == fam].copy()
    d['resid'] = d.lag_median_ms - d.groupby('level_id').lag_median_ms.transform('median')
    oe[fam] = list(stats.spearmanr(d.order_index, d.resid))
res['order_effects'] = oe

# ---------------- Family F: RTT ----------------
d = df[df.family.str.startswith('F_lat')].copy()
x = d.rtt_ms.values; y = d.lag_median_ms.values
ols = stats.linregress(x, y)
n = len(x); tcrit = stats.t.ppf(0.975, n - 2)
resid = y - (ols.intercept + ols.slope * x)
resF = dict(n=int(n), regions=d.region.tolist(), rtt=x.tolist(), lag=y.tolist(),
            slope=ols.slope, slope_ci=[ols.slope - tcrit * ols.stderr, ols.slope + tcrit * ols.stderr],
            intercept=ols.intercept, intercept_ci=[ols.intercept - tcrit * ols.intercept_stderr, ols.intercept + tcrit * ols.intercept_stderr],
            r2=ols.rvalue ** 2, p=ols.pvalue, rmse=float(np.sqrt(np.mean(resid ** 2))), max_abs_resid=float(np.abs(resid).max()),
            spearman=list(stats.spearmanr(x, y)))
# region summary
g = d.groupby('region').agg(rtt=('rtt_ms', 'first'), lag_med=('lag_median_ms', 'median'), lag_min=('lag_median_ms', 'min'), lag_max=('lag_median_ms', 'max'),
                            p95_med=('lag_p95_ms', 'median'), wal=('wal_mbps_level', 'mean'), commits=('commits_per_sec', 'mean'),
                            applied=('sub_rows_applied_per_sec', 'mean'), pubcpu=('pub_cpu_mean', 'mean'), subcpu=('sub_cpu_mean', 'mean'),
                            lagbytes=('lag_bytes_median', 'median')).sort_values('rtt')
resF['region_table'] = g.reset_index().to_dict('records')
# leave-one-region-out
loro = []
for reg in g.index:
    tr = d[d.region != reg]; te = d[d.region == reg]
    o = stats.linregress(tr.rtt_ms, tr.lag_median_ms)
    pred = o.intercept + o.slope * te.rtt_ms.iloc[0]
    loro.append(dict(region=reg, rtt=float(te.rtt_ms.iloc[0]), observed_median=float(te.lag_median_ms.median()), predicted=float(pred),
                     error_ms=float(te.lag_median_ms.median() - pred), slope=o.slope, intercept=o.intercept))
resF['loro'] = loro
# slope test vs 1.0
resF['t_slope_vs_1'] = float((ols.slope - 1.0) / ols.stderr); resF['p_slope_vs_1'] = float(2 * stats.t.sf(abs((ols.slope - 1.0) / ols.stderr), n - 2))
res['F'] = resF

# ---------------- Measured-vs-requested WAL rate (reviewer 12) ----------------
df['wal_MiB_level'] = df.wal_bytes_delta / df.load_seconds / 2 ** 20
df['setpoint_err_MiB_pct'] = 100 * (df.wal_MiB_level / df.target_wal_mbps - 1)
df['commit_agreement_pct'] = 100 * (df.xact_commit_delta / df.loadgen_commits - 1)
res['setpoint'] = dict(abs_err_median_pct=float(df.setpoint_err_MiB_pct.abs().median()), abs_err_max_pct=float(df.setpoint_err_MiB_pct.abs().max()),
                       worst_level=df.loc[df.setpoint_err_MiB_pct.abs().idxmax(), ['run_id', 'level_id', 'wal_MiB_level', 'target_wal_mbps']].to_dict(),
                       commit_agreement_abs_max_pct=float(df.commit_agreement_pct.abs().max()),
                       n_levels=int(len(df)), n_exceeded=int(df.exceeded_capacity.sum()), sql_errors=int(df.sql_errors.sum()),
                       pre_backlog_max_bytes=int(df.pre_start_backlog.max()), post_drain_max_sec=float(df.post_drain_sec.max()))
df.to_csv(os.path.join(OUT, 'runlevel_ext.csv'), index=False)

def conv(o):
    if isinstance(o, (np.floating,)): return float(o)
    if isinstance(o, (np.integer,)): return int(o)
    if isinstance(o, (np.bool_,)): return bool(o)
    if isinstance(o, np.ndarray): return o.tolist()
    raise TypeError(str(type(o)))
json.dump(res, open(os.path.join(OUT, 'stats.json'), 'w'), indent=1, default=conv)
print(json.dumps(res, indent=1, default=conv)[:20000])
