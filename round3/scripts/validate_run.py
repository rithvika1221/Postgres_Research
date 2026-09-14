#!/usr/bin/env python3
"""
Round 3 validator.  STANDALONE: imports nothing from sibling scripts.

Run after every run, while the VMs are still up. Catching a bad run now costs
ten minutes; catching it during analysis costs a day.

Exit codes
----------
  0  clean, or clean with warnings
  1  problems found - affected levels should be re-run
  2  run artefacts missing

Usage
-----
  py validate_run.py --run-id B_concurrency_rep1
  py validate_run.py --run-id B_concurrency_rep1 --out C:\\r3\\data
"""

import argparse
import csv
import json
import os
import statistics as st
from datetime import datetime

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")

# Printed in the header of every report. Without it there is no way to tell
# from an output file which build produced it, and a stale copy in
# C:\\r3\\scripts looks exactly like a clean result.
VALIDATOR_VERSION = "v4 2026-09-06"
VALIDATOR_NOTE = ("workload WAL excludes full-page images; events are "
                  "judged by the phase they occurred in")
EXPECTED_MAJOR = 18


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# --- separating the workload's WAL from the checkpointer's -----------------
#
# The closed loop regulates pg_stat_wal.wal_bytes, which is everything the
# server writes - including full-page images. An FPI is emitted the first time
# a page is touched after a checkpoint, so FPI volume is a property of the
# checkpoint schedule, not of the workload, and the loop cannot influence it.
#
# In family E this made a perfectly-held set point look like a runaway: total
# WAL climbed 32.2 -> 38.2 MB/s across five levels while the workload's own WAL
# stayed flat at ~31.2, because longer levels contain more checkpoints. Judging
# the controller on total WAL therefore condemns runs that were correct.
#
# The split is measured per run rather than assumed:
#     wal_bytes_delta = a * (records - fpi) + b * fpi
# a is the mean size of an ordinary record, b the mean cost of a full-page
# image - about 7 kB, an 8 kB page minus the free-space hole. On the real
# E_duration_rep1 data this two-term model explains R^2 = 0.9989 of the bytes.
FPI_MIN_INTERVALS = 60
FPI_BYTES_MIN, FPI_BYTES_MAX = 1000.0, 8500.0
FPI_MIN_R2 = 0.95


EVENT_PHASE_TOLERANCE_SEC = 5.0


def _iso_epoch(s):
    """Seconds since the epoch from the monitor's ISO timestamps, or None."""
    try:
        t = s.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(t).timestamp()
    except Exception:
        return None


def phase_at(when_iso, rows):
    """(phase_state, level_id) the monitor was recording at that instant.

    Returns (None, None) if no sample is close enough to say - in which case
    the caller must not assume the event was harmless.
    """
    t = _iso_epoch(when_iso)
    if t is None or not rows:
        return None, None
    best, best_d = None, None
    for r in rows:
        e = fnum(r.get("epoch"))
        if e is None:
            continue
        d = abs(e - t)
        if best_d is None or d < best_d:
            best, best_d = r, d
    if best is None or best_d > EVENT_PHASE_TOLERANCE_SEC:
        return None, None
    return best.get("phase_state"), best.get("level_id")


def wal_deltas(rows_by_level):
    """(non-FPI records, FPI records, bytes, seconds) per sampling interval."""
    out = []
    for sub in rows_by_level:
        for a, b in zip(sub, sub[1:]):
            dt = None
            ea, eb = fnum(a.get("epoch")), fnum(b.get("epoch"))
            if ea is not None and eb is not None:
                dt = eb - ea
            dw = fnum(b.get("wal_bytes")), fnum(a.get("wal_bytes"))
            dr = fnum(b.get("wal_records")), fnum(a.get("wal_records"))
            df = fnum(b.get("wal_fpi")), fnum(a.get("wal_fpi"))
            if None in dw + dr + df or dt is None or dt <= 0:
                continue
            W, R, F = dw[0] - dw[1], dr[0] - dr[1], df[0] - df[1]
            if W < 0 or R <= 0 or F < 0 or F > R:
                continue          # a counter reset, or a torn sample
            out.append((R - F, F, W, dt))
    return out


def fit_fpi_cost(deltas):
    """Least squares for (bytes per ordinary record, bytes per FPI, R^2).

    Two-parameter fit through the origin, solved from the normal equations so
    the validator keeps its promise of importing nothing.
    """
    if len(deltas) < FPI_MIN_INTERVALS:
        return None
    s11 = s12 = s22 = s1y = s2y = sy = syy = 0.0
    n = 0
    for x1, x2, y, _dt in deltas:
        s11 += x1 * x1; s12 += x1 * x2; s22 += x2 * x2
        s1y += x1 * y;  s2y += x2 * y
        sy += y;        syy += y * y
        n += 1
    det = s11 * s22 - s12 * s12
    if det == 0 or n < FPI_MIN_INTERVALS:
        return None
    a = (s22 * s1y - s12 * s2y) / det
    b = (s11 * s2y - s12 * s1y) / det
    ybar = sy / n
    sstot = syy - n * ybar * ybar
    ssres = 0.0
    for x1, x2, y, _dt in deltas:
        ssres += (y - a * x1 - b * x2) ** 2
    r2 = 1.0 - ssres / sstot if sstot > 0 else 0.0
    if not (FPI_BYTES_MIN <= b <= FPI_BYTES_MAX) or r2 < FPI_MIN_R2:
        return None
    return a, b, r2, n


def main():
    ap = argparse.ArgumentParser(description="Validate a Round 3 run (standalone).")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--setpoint-tol", type=float, default=0.15)
    ap.add_argument("--ramp-exclude", type=int, default=30,
                    help="samples to discard at the start of each load phase")
    args = ap.parse_args()

    man_p = os.path.join(args.out, f"manifest_{args.run_id}.json")
    pub_p = os.path.join(args.out, f"publisher_{args.run_id}.csv")
    sub_p = os.path.join(args.out, f"subscriber_{args.run_id}.csv")
    evt_p = os.path.join(args.out, f"publisher_{args.run_id}_events.log")

    if not os.path.isfile(man_p):
        print(f"FAIL: no manifest at {man_p}")
        return 2
    with open(man_p, encoding="utf-8") as fh:
        man = json.load(fh)

    problems, warnings = [], []

    rows = []
    if os.path.isfile(pub_p):
        with open(pub_p, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    else:
        problems.append(f"no publisher CSV at {pub_p} — was monitor.py running?")
    if not os.path.isfile(sub_p):
        warnings.append(f"no subscriber CSV at {sub_p}")

    print("=" * 78)
    print(f"RUN {man['run_id']}   PostgreSQL {man.get('pg_version')}   "
          f"seed={man.get('seed')}   randomised={man.get('randomised')}")
    print(f"validator {VALIDATOR_VERSION} - {VALIDATOR_NOTE}")
    print(f"order: {' -> '.join(man.get('level_order', []))}")
    ab = man.get("abort") or {}
    if ab:
        print(f"STATUS: ABORTED [{ab.get('code')}] {ab.get('message')}")
        if ab.get("code") in ("OPERATOR_STOP", "INTERRUPTED"):
            # A deliberate stop is not a data defect. The levels that DID
            # complete are still usable; grading the run FAIL for it trains
            # people to ignore the validator.
            warnings.append(f"run stopped on purpose ({ab.get('code')}); "
                            f"the completed levels are still usable")
        else:
            problems.append(f"run aborted: {ab.get('code')} - {ab.get('message')}")
    print("=" * 78)

    # Did the run actually finish, and did every planned level run? Neither was
    # checked, so a crashed orchestrator that produced an empty results list
    # validated as "PASS - run is clean".
    if not man.get("finished_utc"):
        problems.append("the orchestrator never finished - this manifest is a "
                        "partial write, not a completed run")
    planned = list(man.get("level_order") or [])
    ran = [r["level_id"] for r in man.get("results", [])]
    ok_ids = {r["level_id"] for r in man.get("results", []) if not r.get("failed")}
    missing = [l for l in planned if l not in ok_ids]
    if planned and missing:
        problems.append(f"{len(missing)} of {len(planned)} planned levels have no "
                        f"usable result: {', '.join(missing)}")
    if not ran:
        problems.append("the manifest contains no level results at all")

    sub = man.get("subscriber") or {}
    if not sub:
        # A completed run always records the subscriber. If the key is absent
        # from a run that finished and produced results, the manifest has been
        # edited or truncated - that is a problem, not a historical curiosity.
        if man.get("finished_utc") and man.get("results"):
            problems.append("this manifest has no subscriber record at all, yet "
                            "claims to be a completed run - it has been edited, "
                            "truncated, or produced by an older harness")
        else:
            warnings.append("this manifest predates subscriber capture - the "
                            "subscriber's version, settings and indexes are unknown")
    elif not sub.get("reachable"):
        problems.append(f"the subscriber was not captured: {sub.get('error') or sub.get('note')}")
    elif len(sub.get("indexes") or []) < 4:
        problems.append(f"the subscriber had {len(sub.get('indexes') or [])} indexes "
                        f"on ingest_data, expected 4 - apply cost is understated")

    pv = str(man.get("pg_version", ""))
    if pv and not pv.startswith(str(EXPECTED_MAJOR)):
        problems.append(f"PostgreSQL {pv} — Round 3 is single-version "
                        f"({EXPECTED_MAJOR}); do not mix rounds")

    # Fit the WAL model across every level of the run before judging any of
    # them, so the FPI cost is estimated from as much data as possible.
    load_by_level = []
    for r in man.get("results", []):
        if r.get("failed"):
            continue
        load_by_level.append([x for x in rows
                              if x.get("level_id") == r["level_id"]
                              and x.get("phase_state") == "load"])
    fpi_fit = fit_fpi_cost(wal_deltas(load_by_level))
    if fpi_fit:
        a_rec, b_fpi, r2, nfit = fpi_fit
        print(f"WAL model: {a_rec:.0f} B per ordinary record, {b_fpi:.0f} B per "
              f"full-page image, R^2={r2:.4f} over {nfit:,} intervals")
        print("           set points are judged on workload WAL (wlMB), which "
              "excludes full-page")
        print("           images - those follow the checkpoint schedule, not "
              "the load generator")
    else:
        warnings.append("could not separate full-page images from workload WAL; "
                        "set points are judged on TOTAL WAL, which overstates "
                        "the error on any level containing a checkpoint")

    hdr = (f"{'level':>6} {'cli':>4} {'r/c':>5} {'tgtMB':>6} {'measMB':>7} "
           f"{'wlMB':>7} {'err%':>6} {'clean':>6} {'drainS':>7} {'applyMB':>8} "
           f"{'over':>5} {'err':>4}")
    print(hdr)
    print("-" * len(hdr))

    for r in man.get("results", []):
        lid = r["level_id"]
        if r.get("failed"):
            problems.append(f"{lid}: {r.get('error_code')} — {r.get('error_message')}")
            print(f"{lid:>6}   FAILED  {r.get('error_code')}")
            continue

        p = r.get("params", {})
        tgt = p.get("target_wal_mbps") or 0
        pd_ = r.get("post_drain") or {}
        lg = r.get("loadgen") or {}

        sub = [x for x in rows if x.get("level_id") == lid
               and x.get("phase_state") == "load"]
        # Discard the ramp, but never discard the whole level: a level shorter
        # than the ramp window would otherwise be silently unvalidated and
        # printed as 0.0 MB/s, which reads exactly like a level that produced
        # no load at all.
        tail = sub[args.ramp_exclude:]
        if sub and not tail:
            tail = sub[len(sub) // 2:]
            warnings.append(
                f"{lid}: only {len(sub)} load samples, fewer than the "
                f"{args.ramp_exclude}-sample ramp exclusion; validated on the "
                f"last {len(tail)} instead")
        steady = [fnum(x.get("wal_mb_per_sec")) for x in tail]
        steady = [v for v in steady if v is not None]
        smean = st.mean(steady) if steady else None
        if sub and smean is None:
            warnings.append(f"{lid}: no usable wal_mb_per_sec samples during load")

        # Workload WAL: total minus the full-page images, using the cost fitted
        # above. This is what the closed loop can actually control, so it is
        # what the set point is judged on.
        wmean = None
        if fpi_fit and tail:
            d = wal_deltas([tail])
            if d:
                bytes_tot = sum(x[2] for x in d)
                secs = sum(x[3] for x in d)
                fpi_bytes = sum(x[1] for x in d) * fpi_fit[1]
                if secs > 0:
                    wmean = max(0.0, bytes_tot - fpi_bytes) / secs / 1048576

        judged = wmean if wmean is not None else smean
        err = ((judged - tgt) / tgt) if (tgt and judged is not None) else None
        # This used to be excused whenever exceeded_capacity was true, on the
        # reasoning that a saturated subscriber explains a missed set point. It
        # does not. exceeded_capacity describes the SUBSCRIBER's backlog, and
        # the publisher's WAL byte rate is not throttled by the subscriber -
        # that is the whole premise of an asynchronous replication experiment.
        # The suppression hid every level of family E: E03/E04/E05 ran +15%,
        # +18% and +19% above target and were reported clean, which is exactly
        # the confound the family exists to rule out.
        if err is not None and abs(err) > args.setpoint_tol:
            note = ""
            if r.get("exceeded_capacity"):
                note = ("  (this level also exceeded apply capacity, but that "
                        "does not explain a publisher-side rate miss)")
            basis = ("workload WAL" if wmean is not None else "total WAL")
            problems.append(f"{lid}: closed loop missed set point — target {tgt} MB/s, "
                            f"{basis} {judged:.1f} MB/s ({err*100:+.0f}%){note}")

        if not r.get("started_clean"):
            problems.append(f"{lid}: did not start drained — level is contaminated")
        if not pd_.get("drained"):
            problems.append(f"{lid}: never drained after the load stopped")
        if lg.get("sql_errors"):
            warnings.append(f"{lid}: load generator logged {lg['sql_errors']} SQL errors")
        if not lg.get("completed_commits"):
            problems.append(f"{lid}: zero completed transactions")
        if not sub:
            problems.append(f"{lid}: no monitor samples tagged for this level - "
                            f"the 1 Hz series is the raw data, so this level "
                            f"cannot be analysed")

        # How much of THIS level's load ran with no walsender at all. The
        # event log says an outage happened; only this says whether it touched
        # a measurement, and how much of one. A couple of seconds inside the
        # excluded ramp is nothing; a tenth of the level is not.
        if sub:
            down = sum(1 for x in sub if not (x.get("repl_state") or "").strip())
            if down:
                frac = down / len(sub)
                in_ramp = sum(1 for x in sub[:args.ramp_exclude]
                              if not (x.get("repl_state") or "").strip())
                where = (" - all inside the excluded ramp, so the measured window "
                         "is unaffected") if in_ramp == down else ""
                msg = (f"{lid}: {down} of {len(sub)} load samples ({frac*100:.1f}%) "
                       f"had no row in pg_stat_replication{where}")
                if frac > 0.05 and in_ramp != down:
                    problems.append(msg)
                else:
                    warnings.append(msg)

        man_meas = r.get("measured_wal_mb_per_sec")
        if man_meas is not None and smean is not None and man_meas > 0.05:
            disagree = abs(man_meas - smean) / man_meas
            if disagree > 0.35:
                warnings.append(
                    f"{lid}: the manifest says {man_meas:.2f} MB/s but the 1 Hz "
                    f"samples average {smean:.2f} MB/s ({disagree*100:.0f}% apart) "
                    f"- one of the two windows does not describe this level")
        if man_meas is not None and man_meas < 0:
            problems.append(f"{lid}: negative measured WAL rate ({man_meas}) - the "
                            f"statistics counters were reset mid-level")
        if (r.get("wal_bytes_generated") or 0) < 0:
            problems.append(f"{lid}: negative wal_bytes_generated - counters reset")
        if lg.get("workers_min_alive") is not None and lg.get("workers_started"):
            if lg["workers_min_alive"] < lg["workers_started"]:
                problems.append(f"{lid}: only {lg['workers_min_alive']} of "
                                f"{lg['workers_started']} load generator workers "
                                f"survived - the client count is not what the "
                                f"level claims")
        if lg.get("controller_started") is False:
            problems.append(f"{lid}: the WAL controller never started - this level "
                            f"ran open loop")
        if lg.get("reconnects"):
            warnings.append(f"{lid}: load generator reconnected {lg['reconnects']} "
                            f"times (the publisher dropped connections)")
        if r.get("exceeded_capacity_basis") == "end_of_load_backlog_only":
            warnings.append(f"{lid}: capacity classified from a single end-of-load "
                            f"sample - too few lag samples to fit a slope")

        arate = pd_.get("apply_rate_mb_per_sec")
        arate_s = f"{arate:.1f}" if arate else "-"
        err_s = f"{err*100:+.0f}" if err is not None else "-"
        smean_s = f"{smean:.1f}" if smean is not None else "-"
        wmean_s = f"{wmean:.1f}" if wmean is not None else "-"
        print(f"{lid:>6} {p.get('clients'):>4} {p.get('rows_per_commit'):>5} "
              f"{tgt:>6.0f} {smean_s:>7} {wmean_s:>7} {err_s:>6} "
              f"{('yes' if r.get('started_clean') else 'NO'):>6} "
              f"{pd_.get('drain_sec', 0):>7.0f} {arate_s:>8} "
              f"{('YES' if r.get('exceeded_capacity') else 'no'):>5} "
              f"{lg.get('sql_errors', 0):>4}")

    # ---- data integrity ---------------------------------------------------
    if rows:
        gaps = sum(1 for a, b in zip(rows, rows[1:])
                   if (fnum(a.get('epoch')) and fnum(b.get('epoch'))
                       and fnum(b['epoch']) - fnum(a['epoch']) > 5.0))
        if gaps:
            warnings.append(f"publisher CSV has {gaps} sampling gaps over 5 s")
        missing = sum(1 for x in rows if fnum(x.get("wal_bytes")) is None)
        if missing:
            problems.append(f"{missing} samples have no pg_stat_wal.wal_bytes — "
                            "monitor needs sufficient privilege")
        badrate = sum(1 for x in rows
                      if (fnum(x.get("wal_mb_per_sec")) or 0) < 0
                      or (fnum(x.get("commits_per_sec")) or 0) < 0)
        if badrate:
            problems.append(f"{badrate} samples have negative WAL or commit rates - "
                            f"the server's statistics were reset mid-run")
        neg = sum(1 for x in rows
                  if (fnum(x.get("replay_lag_sec")) or 0) < -0.001)
        if neg:
            warnings.append(f"{neg} samples have negative lag times — check clock sync "
                            "(w32tm /resync on both hosts)")

    if os.path.isfile(evt_p):
        with open(evt_p, encoding="utf-8") as fh:
            evts = [l.split("\t") for l in fh if "\t" in l]
        bad = [e for e in evts if len(e) > 1 and e[1] in
               ("REPLICATION_DOWN", "SLOT_INVALID", "QUERY_ERROR", "QUERY_UNSUPPORTED",
                "DERIVED_RATE_ERROR", "SAMPLE_EXCEPTION", "DISK_LOW", "ABORT",
                "COUNTER_RESET", "ROTATE_FAILED", "FATAL")]
        for e in bad[:10]:
            txt = e[2].strip() if len(e) > 2 else ""
            # Where an event happened decides what it means. A walsender that
            # times out while the harness is truncating a 38-million-row table
            # between levels costs nothing: the slot holds the position, the
            # subscriber resumes from its confirmed LSN, and no measurement was
            # running. The same event during a load phase invalidates that
            # level. Flagging both identically made the operator's only
            # response "re-run 3 hours", for an outage in dead time.
            when, level = phase_at(e[0], rows) if e[1] == "REPLICATION_DOWN" \
                else (None, None)
            if when is not None and when != "load":
                warnings.append(
                    f"monitor event {e[1]} during the '{when}' phase of {level} "
                    f"- between measurements, so no level is contaminated: {txt}")
            else:
                problems.append(f"monitor event {e[1]}: {txt}")
        # A degraded monitor still records; the operator just needs to know
        # which columns are affected before analysing them.
        for e in [x for x in evts if len(x) > 1 and x[1] == "QUERY_DEGRADED"][:5]:
            warnings.append(f"monitor event QUERY_DEGRADED: {e[2].strip() if len(e) > 2 else ''}")
        gapev = [e for e in evts if len(e) > 1 and e[1] == "SAMPLE_GAP"]
        if gapev:
            warnings.append(f"monitor logged {len(gapev)} sampling gaps")
    else:
        warnings.append("no monitor event log found")

    # ---- apply-rate consistency ------------------------------------------
    ap_ = [(r["level_id"], r["post_drain"]["apply_rate_mb_per_sec"])
           for r in man.get("results", [])
           if (r.get("post_drain") or {}).get("apply_rate_mb_per_sec")]
    if len(ap_) >= 3:
        vals = [v for _, v in ap_]
        m, s = st.mean(vals), st.pstdev(vals)
        print(f"\napply rate across {len(vals)} levels: mean {m:.1f} MB/s, sd {s:.1f}")
        if s > m * 0.5:
            warnings.append(f"apply rate varies widely across levels "
                            f"(mean {m:.1f}, sd {s:.1f} MB/s) — expected roughly constant")

    print()
    if problems:
        print(f"PROBLEMS ({len(problems)}):")
        for p in problems:
            print(f"  x {p}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  ! {w}")
    if not problems and not warnings:
        print("PASS — run is clean.")
    elif not problems:
        print("PASS with warnings — usable.")
    else:
        print("\nFAIL — re-run the affected levels before using this data.")

    rp = os.path.join(args.out, f"run_report_{args.run_id}.md")
    if os.path.isfile(rp):
        print(f"\nreport: {rp}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
