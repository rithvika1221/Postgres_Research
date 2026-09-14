#!/usr/bin/env python3
import json, os
import numpy as np, pandas as pd
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

A = '/home/claude/paper/analysis'; FIG = '/home/claude/paper/figs'
df = pd.read_csv(os.path.join(A, 'runlevel_ext.csv')); S = json.load(open(os.path.join(A, 'stats.json')))
plt.rcParams.update({'font.family': 'serif', 'font.serif': ['Times New Roman', 'Liberation Serif', 'DejaVu Serif'], 'font.size': 10,
                     'axes.titlesize': 10, 'axes.labelsize': 10, 'legend.fontsize': 8.5, 'figure.dpi': 300, 'savefig.dpi': 300,
                     'axes.spines.top': False, 'axes.spines.right': False})
MK = ['o', 's', '^']  # replicate markers
COL = {'a': '#1f77b4', 'b': '#d62728', 'c': '#2ca02c', 'd': '#7f7f7f'}

def runs_scatter(ax, d, xcol, ycol, color, label=None, jitter=0.0):
    for i, (rid, g) in enumerate(sorted(d.groupby('run_id'))):
        g = g.sort_values(xcol)
        ax.scatter(g[xcol] * (1 + jitter * (i - 1)), g[ycol], marker=MK[i % 3], s=28, facecolors='none', edgecolors=color, linewidths=1.0,
                   label=(f'{label}, replicate {i+1}' if label else f'replicate {i+1}'), zorder=3)

# ---------- Figure 1: pipeline schematic ----------
fig, ax = plt.subplots(figsize=(6.5, 2.6)); ax.axis('off'); ax.set_xlim(0, 100); ax.set_ylim(0, 40)
def box(x, y, w, h, text, fc='#f2f2f2'):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0.3', fc=fc, ec='black', lw=0.8))
    ax.text(x + w / 2, y + h / 2, text, ha='center', va='center', fontsize=8.5)
def arrow(x1, y1, x2, y2, text=None, dy=2.2, style='-|>', ls='-'):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=style, mutation_scale=10, lw=0.9, ls=ls, color='black'))
    if text: ax.text((x1 + x2) / 2, (y1 + y2) / 2 + dy, text, ha='center', va='bottom', fontsize=7.5)
box(1, 22, 15, 12, 'Load generator\n(1–64 clients)'); box(21, 22, 16, 12, 'Publisher\nWAL buffers'); box(42, 22, 16, 12, 'WAL writer\nflush ≤1 MiB /\n≤200 ms', '#e8f0fa')
box(63, 22, 14, 12, 'walsender\n+ pgoutput\ndecoding', '#e8f0fa'); box(83, 22, 16, 12, 'Subscriber\napply worker', '#e8f0fa')
arrow(16, 28, 21, 28); ax.text(18.5, 35.5, 'commits', ha='center', fontsize=7.5)
arrow(37, 28, 42, 28); arrow(58, 28, 63, 28); ax.text(60.5, 35.5, 'flushed WAL', ha='center', fontsize=7.5)
arrow(77, 28, 83, 28); ax.text(80, 35.5, 'row changes', ha='center', fontsize=7.5)
arrow(91, 22, 91, 6, style='-'); arrow(91, 6, 70, 6, style='-'); arrow(70, 6, 70, 22)
ax.text(80.5, 3.2, 'feedback message: written / flushed / applied LSN', ha='center', va='top', fontsize=7.5)
ax.annotate('', xy=(64, 15), xytext=(97, 15), arrowprops=dict(arrowstyle='<->', lw=0.9, color='#d62728'))
ax.text(80.5, 16.3, 'replay lag (time): from sending an LSN to receiving\nconfirmation that the subscriber applied it', ha='center', va='bottom', fontsize=7.2, color='#d62728')
ax.text(29, 18, 'Publisher samples (1 Hz): pg_stat_wal.wal_bytes,\npg_stat_database.xact_commit and tup_inserted,\npg_stat_replication lag columns', ha='center', va='top', fontsize=7.2)
fig.savefig(os.path.join(FIG, 'fig1_pipeline.png'), bbox_inches='tight'); plt.close(fig)

# ---------- Figure 2: family B ----------
fig, axes = plt.subplots(1, 2, figsize=(6.5, 2.9))
for ax, tgt, key, color in [(axes[0], 8, '8MiB', COL['a']), (axes[1], 30, '30MiB', COL['b'])]:
    d = df[(df.family == 'B_concurrency') & (df.target_wal_mbps == tgt)]
    runs_scatter(ax, d, 'clients', 'lag_median_ms', color, jitter=0.03)
    lv = S['B'][key]; ax.plot(lv['clients'], lv['level_median'], '-', color=color, lw=1.2, label='median of replicates')
    ax.plot(lv['clients'], lv['p95_level_median'], '--', color=color, lw=1.0, label='95th percentile (median of replicates)')
    ax.set_xscale('log', base=2); ax.set_xticks(lv['clients']); ax.set_xticklabels([str(c) for c in lv['clients']])
    ax.set_xlabel('Concurrent clients'); ax.set_title(f'{tgt} MiB/s measured WAL rate'); ax.grid(alpha=0.3, lw=0.5)
    ax2 = ax.twinx(); ax2.plot(lv['clients'], lv['pub_cpu_level_mean'], ':', color='black', lw=1.0, marker='x', ms=4, label='publisher CPU (%)')
    ax2.set_ylim(0, 40); ax2.spines['top'].set_visible(False); ax2.set_ylabel('Publisher CPU (%)')
axes[0].set_ylabel('Replay lag (ms)'); axes[0].set_ylim(10, 30); axes[1].set_ylim(10, 30)
h1, l1 = axes[0].get_legend_handles_labels()
fig.legend(h1 + [plt.Line2D([], [], ls=':', color='black', marker='x', ms=4)], l1 + ['publisher CPU (%)'], loc='lower center', ncol=3, bbox_to_anchor=(0.5, -0.12), frameon=False)
fig.tight_layout(); fig.savefig(os.path.join(FIG, 'fig2_concurrency.png'), bbox_inches='tight'); plt.close(fig)

# ---------- Figure 3: family C ----------
fig, ax = plt.subplots(figsize=(6.0, 3.0))
d = df[df.family == 'C_batching']; lv = S['C']
runs_scatter(ax, d, 'rows_per_commit', 'lag_median_ms', COL['a'], 'median', jitter=0.04)
ax.plot(lv['rows_per_commit'], lv['level_median'], '-', color=COL['a'], lw=1.2, label='median of replicates')
for i, (rid, g) in enumerate(sorted(d.groupby('run_id'))):
    g = g.sort_values('rows_per_commit'); ax.scatter(g.rows_per_commit * (1 + 0.04 * (i - 1)), g.lag_p95_ms, marker=MK[i], s=28, color=COL['b'], linewidths=0.8, label=f'95th percentile, replicate {i+1}', zorder=3)
ax.plot(lv['rows_per_commit'], lv['p95_level_median'], '--', color=COL['b'], lw=1.0, label='95th percentile, median of replicates')
ax.set_xscale('log'); ax.set_xticks(lv['rows_per_commit']); ax.set_xticklabels([str(c) for c in lv['rows_per_commit']])
ax.set_xlabel('Rows per commit (measured WAL rate held at 16 MiB/s)'); ax.set_ylabel('Replay lag (ms)'); ax.grid(alpha=0.3, lw=0.5); ax.set_ylim(0, 65)
ax.legend(ncol=2, frameon=False, loc='upper left')
fig.tight_layout(); fig.savefig(os.path.join(FIG, 'fig3_batching.png'), bbox_inches='tight'); plt.close(fig)

# ---------- Figure 4: family D ----------
fig, ax = plt.subplots(figsize=(6.0, 3.0))
d = df[df.family == 'D_rowsize']; lv = S['D']
runs_scatter(ax, d, 'row_bytes', 'lag_median_ms', COL['a'], jitter=0.04)
ax.plot(lv['row_bytes'], lv['level_median'], '-', color=COL['a'], lw=1.2, label='median of replicates')
ax.plot(lv['row_bytes'], lv['p95_level_median'], '--', color=COL['a'], lw=1.0, label='95th percentile (median of replicates)')
ax.set_xscale('log'); ax.set_xticks(lv['row_bytes']); ax.set_xticklabels(['100 B', '1 kB', '10 kB', '50 kB', '100 kB', '250 kB'])
ax.set_xlabel('Row size (measured WAL rate held at 6 MiB/s, one row per commit)'); ax.set_ylabel('Replay lag (ms)'); ax.grid(alpha=0.3, lw=0.5); ax.set_ylim(0, 60)
ax2 = ax.twinx(); g = d.groupby('row_bytes').loadgen_commits_per_sec.mean()
ax2.plot(g.index, g.values, ':', color='black', marker='x', ms=4, lw=1.0, label='commits per second (right axis)'); ax2.set_yscale('log'); ax2.set_ylabel('Commits per second'); ax2.spines['top'].set_visible(False)
h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels(); ax.legend(h1 + h2, l1 + l2, frameon=False, loc='upper right', ncol=1)
fig.tight_layout(); fig.savefig(os.path.join(FIG, 'fig4_rowsize.png'), bbox_inches='tight'); plt.close(fig)

# ---------- Figure 5: family F ----------
fig, (ax, axr) = plt.subplots(2, 1, figsize=(6.0, 4.2), gridspec_kw={'height_ratios': [3, 1.1]}, sharex=True)
d = df[df.family.str.startswith('F_lat')]; F = S['F']
names = {'centralus': 'Central US', 'eastus': 'East US', 'northeurope': 'North Europe', 'centralindia': 'Central India'}
for i, (rid, g) in enumerate(sorted(d.groupby('repeat'))):
    ax.scatter(g.rtt_ms, g.lag_median_ms, marker=MK[i], s=34, facecolors='none', edgecolors=COL['a'], linewidths=1.0, label=f'replicate {i+1}', zorder=3)
xx = np.linspace(0, 215, 50); ax.plot(xx, F['intercept'] + F['slope'] * xx, '-', color='black', lw=1.0, label=f"OLS: lag = {F['intercept']:.1f} + {F['slope']:.3f} × RTT  (R² = {F['r2']:.4f})")
ax.plot(xx, xx, ':', color='gray', lw=0.9, label='lag = RTT (reference)')
for reg, r in d.groupby('region'):
    ax.annotate(names[reg], (r.rtt_ms.iloc[0], r.lag_median_ms.median()), xytext=(6, -12), textcoords='offset points', fontsize=8)
ax.set_ylabel('Replay lag, run median (ms)'); ax.grid(alpha=0.3, lw=0.5); ax.legend(frameon=False, loc='upper left'); ax.set_ylim(0, 245)
res = d.lag_median_ms - (F['intercept'] + F['slope'] * d.rtt_ms)
for i, (rid, g) in enumerate(sorted(d.groupby('repeat'))):
    axr.scatter(g.rtt_ms, res[g.index], marker=MK[i], s=30, facecolors='none', edgecolors=COL['a'], linewidths=1.0)
axr.axhline(0, color='black', lw=0.8); axr.set_ylabel('Residual (ms)'); axr.set_xlabel('Measured round-trip time, publisher → subscriber (ms)'); axr.grid(alpha=0.3, lw=0.5); axr.set_ylim(-8, 8)
fig.tight_layout(); fig.savefig(os.path.join(FIG, 'fig5_rtt.png'), bbox_inches='tight'); plt.close(fig)

# ---------- Figure 6: byte lag vs time lag excerpt ----------
pub = pd.read_csv('/home/claude/r3data/F/publisher_F_lat_centralus_rep1.csv')
p = pub[(pub.level_id == 'F01') & (pub.phase_state == 'load')].copy(); p['t'] = p.epoch - p.epoch.min(); p = p[(p.t >= 120) & (p.t <= 240)]
fig, (a1, a2) = plt.subplots(2, 1, figsize=(6.0, 3.6), sharex=True)
a1.step(p.t, p.replay_lag_bytes / 1024, where='post', color=COL['d'], lw=0.9); a1.set_ylabel('Replay lag (KiB)'); a1.axhline(1024, color=COL['b'], ls='--', lw=0.8); a1.text(200, 1024 + 120, 'wal_writer_flush_after = 1 MiB', color=COL['b'], fontsize=8)
a1.set_ylim(0, 1300); a1.grid(alpha=0.3, lw=0.5)
a2.plot(p.t, p.replay_lag_sec * 1000, '-', color=COL['a'], lw=0.9); a2.set_ylabel('Replay lag (ms)'); a2.set_xlabel('Time within level (s); Central US subscriber, 6 MiB/s, one row per commit'); a2.set_ylim(0, 40); a2.grid(alpha=0.3, lw=0.5)
fig.tight_layout(); fig.savefig(os.path.join(FIG, 'fig6_bytes_vs_time.png'), bbox_inches='tight'); plt.close(fig)
print('figures written')
