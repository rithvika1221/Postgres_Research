#!/usr/bin/env python3
"""
Round 3 load generator.  STANDALONE: imports nothing from sibling scripts.

Holds MEASURED WAL generation rate at a set point using PI feedback on
pg_stat_wal.wal_bytes, adjusting the commit rate as rows-per-commit changes.

Why this matters: the Round 2 generator targeted a requested commit rate. When
the system saturated, the achieved rate silently collapsed - 33 commits/s
against a 500/s target at 1,000 rows/commit - while the analysis kept using the
target. Batch size and delivered data rate could not be separated. Controlling
on measured WAL makes "same bytes, different grouping" an actual experiment.

Exit codes
----------
  0  completed normally
  3  too many SQL errors (--max-sql-errors)
  4  completed zero transactions
  5  could not connect / bad arguments / fatal setup error
  6  worker processes died - the load was not delivered
  7  server statistics were reset mid-level (publisher restarted)
  8  the WAL controller never ran - the level would have been open loop

Usage
-----
  set R3_PUB_DSN=host=localhost dbname=pub user=postgres password=...

  py loadgen.py --clients 16 --rows-per-commit 100 --row-bytes 1000 \
                --target-wal-mbps 45 --duration 360 --level-id C04
"""

import argparse
import json
import multiprocessing as mp
import os
import random
import signal
import statistics
import string
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")

# Closed-loop gains.
#
# The manipulated variable is a commit-rate budget in commits/s. Depending on
# batch size and row width that budget is legitimately anywhere from ~10 to
# ~20,000, while the control error is in MB/s. ABSOLUTE gains cannot span that
# range: with the previous KP=0.35 the loop moved the budget by well under one
# commit/s per interval, so a level that started 50% off target was still 50%
# off when it ended - the "closed loop" was open in practice.
#
# These gains act on the RATIO of target to measured rate, so they are
# dimensionless and converge in the same two or three control intervals
# whatever the scale. KP_REL is the fraction of the ideal multiplicative
# correction applied per interval; KI_REL removes the residual bias.
KP_REL = 0.60
KI_REL = 0.04
MAX_RATIO_STEP = 2.0          # never more than double or halve in one interval
INTEGRAL_CLAMP = 0.5          # in budget-fractions; prevents windup
INTEGRAL_LEAK = 0.92          # forget the start-up transient in ~25 s
SMOOTH_ALPHA = 0.5            # EMA on the measured rate, for the control law only
SETPOINT_TOL = 0.15           # how far off target still counts as "on target"
CONNECT_TIMEOUT = 10          # every connect gets one; a blackholed port used
                              # to hang the whole campaign indefinitely
WORKER_GRACE_SECONDS = 60.0   # a worker self-terminates this long after its
                              # level should have ended, whatever the parent did
CONTROL_INTERVAL = 2.0
RAMP_SECONDS = 30
PROGRESS_INTERVAL = 30.0      # seconds between the load generator's progress lines

# Seed for the very first budget: measured WAL cost per row for this table
# (13 columns, jsonb, uuid, three secondary indexes) is roughly the payload
# width plus ~450 bytes of tuple, index and WAL-record overhead. It only has
# to be within about 2x - the controller corrects the rest within a few
# seconds - but a closer seed shortens the transient.
ROW_OVERHEAD_BYTES = 450

REGIONS = ["us-west", "us-east", "eu-west", "ap-south", "sa-east"]
STATUSES = ["pending", "active", "settled", "cancelled", "failed"]
EVENTS = ["order.created", "order.updated", "payment.captured",
          "shipment.dispatched", "refund.issued"]

INSERT_SQL = """
INSERT INTO ingest_data
  (account_id, region_code, status, event_type, quantity,
   unit_price, total_amount, external_ref, attributes, description)
VALUES %s
"""
UPDATE_SQL = """
UPDATE ingest_data SET status = %s, updated_at = now()
WHERE id IN (SELECT id FROM ingest_data ORDER BY id DESC LIMIT %s)
"""
DELETE_SQL = """
DELETE FROM ingest_data
WHERE id IN (SELECT id FROM ingest_data ORDER BY id ASC LIMIT %s)
"""

ALPHABET = string.ascii_letters + string.digits


def _parent_alive(pid):
    """True if the process that started us is still there.

    Windows has no os.kill(pid, 0) semantics we can rely on and no getppid
    that survives, so psutil is used when available and the check simply
    passes when it is not - the absolute deadline still bounds the worker.
    """
    try:
        if os.name == "nt":
            try:
                import psutil
            except ImportError:
                return True
            return psutil.pid_exists(pid)
        return os.getppid() == pid
    except Exception:
        return True


def build_rows(n, row_bytes):
    rows = []
    for _ in range(n):
        q = random.randint(1, 500)
        price = round(random.uniform(1, 999), 4)
        rows.append((
            random.randint(1, 100_000),
            random.choice(REGIONS),
            random.choice(STATUSES),
            random.choice(EVENTS),
            q, price, round(q * price, 4),
            str(uuid.uuid4()),
            json.dumps({"src": "loadgen", "v": 3, "seq": random.randint(1, 10**9)}),
            "".join(random.choices(ALPHABET, k=max(1, row_bytes))),
        ))
    return rows


# --------------------------------------------------------------------------

def _connect_with_retry(cfg, wid, err_msgs, stop_flag):
    """Keep trying to connect until it works or we are told to stop."""
    backoff = 1.0
    while not stop_flag.value:
        try:
            conn = psycopg2.connect(cfg["dsn"], connect_timeout=10,
                                   application_name="r3_loadgen")
            conn.set_session(autocommit=False)
            return conn, conn.cursor()
        except Exception as exc:
            try:
                err_msgs.put(f"worker {wid} connect failed ({type(exc).__name__}), "
                             f"retry in {backoff:.0f}s", block=False)
            except Exception:
                pass
            time.sleep(backoff)
            backoff = min(30.0, backoff * 2)
    return None, None


def worker(wid, cfg, budget, counters, stop_flag, err_msgs, alive):
    random.seed(cfg["seed"] + wid)
    # Absolute deadline. stop_flag is set by the parent, so if the parent is
    # killed - which is exactly what the orchestrator's wall-clock cap does -
    # nothing would ever clear it and these workers would keep writing to the
    # publisher for the rest of the campaign, silently contaminating every
    # level that follows. The deadline and the parent check below make a
    # worker outliving its parent impossible.
    deadline = time.monotonic() + float(cfg["duration"]) + WORKER_GRACE_SECONDS
    parent_pid = cfg.get("parent_pid")

    conn, cur = _connect_with_retry(cfg, wid, err_msgs, stop_flag)
    if conn is None:
        return
    with alive.get_lock():
        alive.value += 1

    tokens, last = 0.0, time.monotonic()
    next_parent_check = 0.0
    try:
        while not stop_flag.value:
            now = time.monotonic()
            if now > deadline:
                break
            if now > next_parent_check:
                next_parent_check = now + 2.0
                if parent_pid is not None and not _parent_alive(parent_pid):
                    break
            share = budget.value / max(1, cfg["clients"])

            if cfg["unthrottled"]:
                allowed = True
            else:
                # max(0.0, ...) matters: a backward clock step used to drive
                # this deeply negative and stall the worker for the size of
                # the step. monotonic makes that impossible, the clamp keeps
                # it impossible.
                tokens = max(0.0, min(tokens + (now - last) * share,
                                      max(1.0, share)))
                allowed = tokens >= 1.0
            last = now
            if not allowed:
                time.sleep(0.001)
                continue

            try:
                if cfg["op_mix"] == "insert":
                    rows = build_rows(cfg["rows_per_commit"], cfg["row_bytes"])
                    psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=1000)
                    ins, upd, dele = cfg["rows_per_commit"], 0, 0
                else:
                    n_i = max(1, int(cfg["rows_per_commit"] * 0.5))
                    n_u = max(0, int(cfg["rows_per_commit"] * 0.3))
                    n_d = max(0, cfg["rows_per_commit"] - n_i - n_u)
                    psycopg2.extras.execute_values(
                        cur, INSERT_SQL, build_rows(n_i, cfg["row_bytes"]), page_size=1000)
                    if n_u:
                        cur.execute(UPDATE_SQL, (random.choice(STATUSES), n_u))
                    if n_d:
                        cur.execute(DELETE_SQL, (n_d,))
                    ins, upd, dele = n_i, n_u, n_d

                conn.commit()
                if not cfg["unthrottled"]:
                    tokens -= 1.0
                with counters.get_lock():
                    counters[0] += 1
                    counters[1] += ins
                    counters[2] += upd
                    counters[3] += dele

            except Exception as exc:
                broken = conn.closed != 0 or isinstance(
                    exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
                try:
                    if not broken:
                        conn.rollback()
                except Exception:
                    broken = True
                with counters.get_lock():
                    # A dropped connection is not a defective statement. The
                    # error budget exists to stop a level that is producing
                    # garbage; one PostgreSQL restart with 16 clients used to
                    # produce 16 "SQL errors" and abort a healthy level.
                    if broken:
                        counters[5] += 1
                        n = 0
                    else:
                        counters[4] += 1
                        n = counters[4]
                if n and n <= 20:
                    try:
                        err_msgs.put(f"worker {wid}: {type(exc).__name__}: "
                                     f"{str(exc)[:160]}", block=False)
                    except Exception:
                        pass
                if broken:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    conn, cur = _connect_with_retry(cfg, wid, err_msgs, stop_flag)
                    if conn is None:
                        break
                    try:
                        err_msgs.put(f"worker {wid} reconnected", block=False)
                    except Exception:
                        pass
                time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        with alive.get_lock():
            alive.value -= 1
        # Without this the process can hang at exit waiting for the queue's
        # feeder thread to flush to a parent that is already gone, holding a
        # publisher connection open in the meantime.
        try:
            err_msgs.cancel_join_thread()
        except Exception:
            pass
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception:
            pass


def controller(cfg, budget, stop_flag, trace_q, ctl_state):
    # The controller used to give up on a single failed connect and return
    # silently. The run then continued at whatever the seed estimate happened
    # to be, still labelled "closed_loop", still exit 0. It races the workers
    # for connection slots and it starts right after the orchestrator has been
    # hammering the server, so that failure is realistic - it must retry, and
    # main() must be able to tell that it never started.
    conn = cur = None
    for attempt in range(6):
        if stop_flag.value:
            return
        try:
            conn = psycopg2.connect(cfg["dsn"], connect_timeout=10,
                                   application_name="r3_loadgen")
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            cur.execute("SELECT wal_bytes::bigint FROM pg_stat_wal")
            prev_b, prev_t = cur.fetchone()[0], time.monotonic()
            break
        except Exception:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = cur = None
            time.sleep(min(10.0, 1.0 * (attempt + 1)))
    if conn is None:
        ctl_state.value = -1
        return
    ctl_state.value = 1

    integral = 0.0
    mbps_f = None
    while not stop_flag.value:
        time.sleep(CONTROL_INTERVAL)
        try:
            cur.execute("SELECT wal_bytes::bigint FROM pg_stat_wal")
            b, t = cur.fetchone()[0], time.monotonic()
        except Exception:
            try:
                cur.close(); conn.close()
            except Exception:
                pass
            try:
                conn = psycopg2.connect(cfg["dsn"], connect_timeout=10,
                                   application_name="r3_loadgen")
                conn.set_session(autocommit=True)
                cur = conn.cursor()
                cur.execute("SELECT wal_bytes::bigint FROM pg_stat_wal")
                prev_b, prev_t = cur.fetchone()[0], time.monotonic()
            except Exception:
                time.sleep(5)
            continue
        dt = t - prev_t
        if dt <= 0:
            continue
        try:
            delta = float(b) - float(prev_b)
        except Exception:
            prev_b, prev_t = b, t
            continue
        if delta < 0:
            # pg_stat_wal was reset - the server crashed and recovered, or
            # someone called pg_stat_reset_shared('wal'). Re-baseline instead
            # of feeding a huge negative rate into the loop.
            ctl_state.value = 2
            prev_b, prev_t = b, t
            continue
        mbps = delta / dt / 1_048_576
        prev_b, prev_t = b, t

        target = cfg["target_wal_mbps"]
        err_rel = (target - mbps) / target

        # Control on a smoothed rate. A checkpoint's full-page-image burst can
        # briefly show 5-10x the steady rate; acting on that raw sample throws
        # the budget off a cliff and takes half a minute to recover. The raw
        # value is still what goes into the trace and the reported statistics.
        mbps_f = mbps if mbps_f is None else (
            SMOOTH_ALPHA * mbps + (1.0 - SMOOTH_ALPHA) * mbps_f)

        # Multiplicative correction: if we are producing half the bytes we
        # want, we need roughly twice the commit rate. Because the correction
        # is a ratio, the loop settles with no steady-state offset, so the
        # integral term only has to clean up slow drift - it is deliberately
        # weak, leaky and clamped, because a stiff integrator here is what
        # turns one bad sample into a minute of wrong load.
        ratio = (target / mbps_f) if mbps_f > 0.02 else MAX_RATIO_STEP
        ratio = max(1.0 / MAX_RATIO_STEP, min(MAX_RATIO_STEP, ratio))

        err_i = max(-1.0, min(1.0, err_rel))
        integral = max(-INTEGRAL_CLAMP,
                       min(INTEGRAL_CLAMP,
                           INTEGRAL_LEAK * integral + err_i * dt / 6.0))

        cur_budget = budget.value
        new = (cur_budget
               + KP_REL * (cur_budget * ratio - cur_budget)
               + KI_REL * integral * cur_budget)
        new = max(0.5, min(cfg["max_commit_rate"], new))
        if new >= cfg["max_commit_rate"] or new <= 0.5:
            integral *= 0.5          # anti-windup at the rails
        budget.value = new
        try:
            trace_q.put({"epoch": t, "measured_wal_mbps": round(mbps, 3),
                         "smoothed_mbps": round(mbps_f, 3),
                         "error_rel": round(err_rel, 3),
                         "commit_budget": round(budget.value, 2),
                         "at_rail": bool(new >= cfg["max_commit_rate"]
                                         or new <= 0.5)}, block=False)
        except Exception:
            pass
    try:
        trace_q.cancel_join_thread()
    except Exception:
        pass
    try:
        cur.close(); conn.close()
    except Exception:
        pass


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Round 3 load generator (standalone).")
    ap.add_argument("--dsn", default=None, help="defaults to $R3_PUB_DSN")
    ap.add_argument("--clients", type=int, required=True)
    ap.add_argument("--rows-per-commit", type=int, default=1)
    ap.add_argument("--row-bytes", type=int, default=1000,
                    help="length of the variable-width description column")
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--op-mix", choices=["insert", "mixed"], default="insert")

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--target-wal-mbps", type=float, default=0.0)
    g.add_argument("--commit-rate", type=float, default=0.0)
    g.add_argument("--unthrottled", action="store_true")

    ap.add_argument("--max-commit-rate", type=float, default=20000.0)
    ap.add_argument("--max-sql-errors", type=int, default=10,
                    help="abort with exit 3 once this many SQL errors occur")
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--level-id", required=True)
    args = ap.parse_args()

    dsn = args.dsn or os.environ.get("R3_PUB_DSN")
    if not dsn:
        print("ERROR: no DSN. Pass --dsn or set R3_PUB_DSN.")
        return 5

    # argparse's mutually exclusive group is satisfied by an explicit ZERO,
    # and both numeric options default to 0.0, so "--target-wal-mbps 0" used
    # to fall through to a full-throttle run labelled open_loop. Levels come
    # from JSON matrices, where a missing or zero field is easy to write.
    chosen = [args.target_wal_mbps > 0, args.commit_rate > 0, bool(args.unthrottled)]
    if sum(chosen) != 1:
        print("ERROR: give exactly one of --target-wal-mbps > 0, "
              "--commit-rate > 0, or --unthrottled.")
        return 5
    if args.duration <= 0 or args.clients < 1 or args.rows_per_commit < 1:
        print("ERROR: --duration, --clients and --rows-per-commit must be positive.")
        return 5

    os.makedirs(args.out, exist_ok=True)
    random.seed(args.seed)

    # sanity-check the target before spawning anything
    try:
        c = psycopg2.connect(dsn, connect_timeout=CONNECT_TIMEOUT,
                            application_name="r3_loadgen")
        c.set_session(autocommit=True)
        k = c.cursor()
        k.execute("SELECT current_setting('server_version'), "
                  "(SELECT wal_bytes::bigint FROM pg_stat_wal), "
                  "(SELECT stats_reset FROM pg_stat_wal)")
        pgv, wal_start, wal_reset_start = k.fetchone()
        k.close(); c.close()
    except Exception as exc:
        print(f"ERROR: cannot connect: {exc}")
        return 5

    if args.commit_rate > 0:
        start = args.commit_rate
    elif args.target_wal_mbps > 0:
        est = max(200, args.row_bytes + ROW_OVERHEAD_BYTES)
        start = max(1.0, (args.target_wal_mbps * 1_048_576) /
                    (est * max(1, args.rows_per_commit)))
    else:
        start = args.max_commit_rate

    cfg = {"dsn": dsn, "clients": args.clients,
           "rows_per_commit": args.rows_per_commit, "row_bytes": args.row_bytes,
           "op_mix": args.op_mix, "seed": args.seed,
           "unthrottled": bool(args.unthrottled),
           "target_wal_mbps": args.target_wal_mbps,
           "max_commit_rate": args.max_commit_rate,
           "duration": args.duration,
           "parent_pid": os.getpid()}

    mode = ("closed_loop" if args.target_wal_mbps else
            "unthrottled" if args.unthrottled else "open_loop")
    print(f"[loadgen] level={args.level_id} pg={pgv} clients={args.clients} "
          f"rows/commit={args.rows_per_commit} row_bytes={args.row_bytes}")
    print(f"[loadgen] mode={mode} "
          + (f"target={args.target_wal_mbps} MB/s " if args.target_wal_mbps else "")
          + f"duration={args.duration}s")

    budget = mp.Value("d", start)
    stop_flag = mp.Value("i", 0)
    # "q" (long long), not "l": ctypes.c_long is 4 bytes on Windows x64, so
    # the row counter would wrap to negative after 2.1e9 rows.
    counters = mp.Array("q", [0, 0, 0, 0, 0, 0])
    err_msgs = mp.Queue()
    trace_q = mp.Queue()
    alive = mp.Value("i", 0)

    ctl_state = mp.Value("i", 0)

    procs = [mp.Process(target=worker,
                        args=(i, cfg, budget, counters, stop_flag, err_msgs, alive))
             for i in range(args.clients)]
    for p in procs:
        p.daemon = True          # a clean parent exit reaps them
        p.start()
    ctl = None
    if args.target_wal_mbps > 0:
        ctl = mp.Process(target=controller,
                         args=(cfg, budget, stop_flag, trace_q, ctl_state))
        ctl.daemon = True
        ctl.start()

    # SIGTERM is how the orchestrator enforces its wall-clock cap. Without a
    # handler the parent dies instantly, the finally block never runs, and the
    # workers survive as orphans writing to the publisher for the rest of the
    # campaign. Turning it into an exception makes the normal shutdown path
    # run instead.
    def _on_term(_sig, _frm):
        raise KeyboardInterrupt("terminated")
    for _sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGBREAK", None)):
        if _sig is not None:
            try:
                signal.signal(_sig, _on_term)
            except Exception:
                pass

    t0 = time.monotonic()
    t0_wall = time.time()
    aborted = None
    trace, errors = [], []
    workers_min_alive = args.clients
    next_progress = PROGRESS_INTERVAL
    try:
        while time.monotonic() - t0 < args.duration:
            time.sleep(1.0)
            while not trace_q.empty():
                try:
                    trace.append(trace_q.get_nowait())
                except Exception:
                    break
            while not err_msgs.empty():
                try:
                    errors.append(err_msgs.get_nowait())
                except Exception:
                    break
            with counters.get_lock():
                n_err, n_com = counters[4], counters[0]
            if n_err >= args.max_sql_errors:
                aborted = f"{n_err} SQL errors (limit {args.max_sql_errors})"
                print(f"[loadgen] ABORT: {aborted}")
                break
            live = sum(1 for pr in procs if pr.is_alive())
            workers_min_alive = min(workers_min_alive, live)
            # ANY worker loss invalidates the level. The load actually
            # delivered is proportional to the survivors, because each worker
            # is throttled to budget/clients - so a level that lost half its
            # workers records a 4-client result under an 8-client label, and
            # nothing downstream can tell. The old rule tolerated exactly half
            # the workers dying and was disabled for the first 60 seconds.
            if live < args.clients:
                aborted = f"only {live}/{args.clients} worker processes still alive"
                print(f"[loadgen] ABORT: {aborted}")
                break
            el = time.monotonic() - t0
            # A deadline, not `int(el) % 30 == 0`: an iteration that takes a
            # little over a second steps int(el) past the multiple, and the
            # line is silently never printed.
            if el >= next_progress:
                next_progress += PROGRESS_INTERVAL
                m = trace[-1]["measured_wal_mbps"] if trace else 0
                pct = 100.0 * el / args.duration
                left = max(0.0, args.duration - el)
                print(f"  [{el:5.0f}s {pct:3.0f}%] {n_com:>9,} commits  "
                      f"{budget.value:7.0f}/s budget  {m:5.1f} MB/s  "
                      f"{left:.0f}s left")
    except KeyboardInterrupt:
        aborted = "interrupted by user"
    finally:
        stop_flag.value = 1
        for p in procs:
            p.join(timeout=15)
            if p.is_alive():
                p.terminate()
        if ctl:
            ctl.join(timeout=10)
            if ctl.is_alive():
                ctl.terminate()
        while not trace_q.empty():
            try:
                trace.append(trace_q.get_nowait())
            except Exception:
                break
        while not err_msgs.empty():
            try:
                errors.append(err_msgs.get_nowait())
            except Exception:
                break

    elapsed = time.monotonic() - t0
    with counters.get_lock():
        commits, ins, upd, dele, errs, reconnects = list(counters)

    wal_reset_end = None
    try:
        c = psycopg2.connect(dsn, connect_timeout=CONNECT_TIMEOUT,
                            application_name="r3_loadgen")
        c.set_session(autocommit=True)
        k = c.cursor()
        k.execute("SELECT wal_bytes::bigint, stats_reset FROM pg_stat_wal")
        wal_end, wal_reset_end = k.fetchone()
        k.close(); c.close()
    except Exception:
        wal_end = None

    # A publisher crash-recovery resets pg_stat_wal, which used to produce a
    # negative wal_bytes_generated and a negative MB/s in the summary, with
    # aborted=null and exit 0.
    counters_reset = (wal_reset_end is not None and wal_reset_start is not None
                      and wal_reset_end != wal_reset_start)
    if (wal_end is not None and wal_start is not None
            and float(wal_end) < float(wal_start)):
        counters_reset = True
    if counters_reset:
        wal_end = None
        if not aborted:
            aborted = "pg_stat_wal was reset during the level (server restart?)"
        print(f"[loadgen] ABORT: {aborted}")

    steady = [t["measured_wal_mbps"] for t in trace
              if t["epoch"] - t0 > RAMP_SECONDS]
    steady_window = f"after {RAMP_SECONDS}s"
    if trace and not steady:
        # A level shorter than the ramp window would otherwise report no
        # steady statistics at all, which reads downstream as "the loop was
        # never measured" and silences the on-target check.
        steady = [t["measured_wal_mbps"] for t in trace[len(trace) // 2:]]
        steady_window = f"last {len(steady)} of {len(trace)} samples "\
                        f"(level shorter than the {RAMP_SECONDS}s ramp)"
    summary = {
        "level_id": args.level_id,
        "written_utc": datetime.now(timezone.utc).isoformat(),
        "pg_version": pgv,
        "mode": mode,
        "aborted": aborted,
        "params": {"clients": args.clients, "rows_per_commit": args.rows_per_commit,
                   "row_bytes": args.row_bytes, "op_mix": args.op_mix,
                   "target_wal_mbps": args.target_wal_mbps,
                   "commit_rate_setpoint": args.commit_rate,
                   "duration_requested_sec": args.duration, "seed": args.seed},
        "elapsed_sec": round(elapsed, 2),
        "ramp_seconds_excluded": RAMP_SECONDS,
        "steady_window": steady_window,
        # client-side completion counts
        "completed_commits": commits,
        "completed_rows_inserted": ins,
        "completed_rows_updated": upd,
        "completed_rows_deleted": dele,
        "sql_errors": errs,
        "reconnects": reconnects,
        "workers_started": args.clients,
        "workers_min_alive": workers_min_alive,
        "controller_started": (ctl_state.value != 0 if ctl else None),
        "counters_reset_during_level": counters_reset,
        "error_samples": errors[:20],
        "achieved_commits_per_sec": round(commits / elapsed, 2) if elapsed else 0,
        "achieved_rows_per_sec": round((ins + upd + dele) / elapsed, 2) if elapsed else 0,
        # server-side WAL over the same window
        "wal_bytes_generated": (wal_end - wal_start) if wal_end else None,
        "measured_wal_mb_per_sec_overall": (
            round((wal_end - wal_start) / elapsed / 1_048_576, 3)
            if wal_end and elapsed else None),
        "control": {
            "samples": len(trace),
            "steady_mean_mbps": round(statistics.mean(steady), 3) if steady else None,
            "steady_stdev_mbps": round(statistics.pstdev(steady), 3) if len(steady) > 1 else None,
            "steady_min_mbps": round(min(steady), 3) if steady else None,
            "steady_max_mbps": round(max(steady), 3) if steady else None,
            "final_commit_budget": round(budget.value, 2),
            "at_rail_fraction": (
                round(sum(1 for t in trace if t.get("at_rail")) / len(trace), 3)
                if trace else None),
        },
        # Did the closed loop actually hold its set point? Without this the
        # summary looks identical whether the target was met or missed by 3x,
        # which is the exact Round 2 failure this generator exists to prevent.
        "on_target": None,
        "setpoint_error_frac": None,
        "trace": trace,
    }
    sm = summary["control"]["steady_mean_mbps"]
    if mode == "closed_loop" and sm is not None and args.target_wal_mbps > 0:
        ef = (sm - args.target_wal_mbps) / args.target_wal_mbps
        summary["setpoint_error_frac"] = round(ef, 4)
        summary["on_target"] = bool(abs(ef) <= SETPOINT_TOL)

    if ctl is not None and ctl_state.value <= 0:
        msg = ("the WAL controller never connected - the level ran OPEN LOOP"
               if ctl_state.value == -1 else
               "the WAL controller produced no samples")
        errors.append(msg)
        summary["error_samples"] = errors[:20]
        if not aborted:
            aborted = msg
            summary["aborted"] = aborted
        print(f"[loadgen] ABORT: {msg}")

    path = os.path.join(args.out, f"loadgen_{args.level_id}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    os.replace(tmp, path)      # a kill mid-write must not leave a half file

    print(f"[loadgen] {commits:,} commits, {ins:,} rows, {errs} SQL errors, "
          f"{elapsed:.0f}s")
    if summary["control"]["steady_mean_mbps"] is not None:
        cst = summary["control"]
        print(f"[loadgen] measured WAL {cst['steady_mean_mbps']} "
              f"+/- {cst['steady_stdev_mbps']} MB/s (steady window)")
    print(f"[loadgen] -> {path}")

    if aborted and "worker" in aborted:
        return 6
    if errs >= args.max_sql_errors:
        return 3
    if commits == 0:
        print("[loadgen] ERROR: zero transactions completed")
        return 4
    if counters_reset:
        print("[loadgen] ERROR: server statistics were reset during the level")
        return 7
    if ctl is not None and ctl_state.value <= 0:
        return 8
    if summary["on_target"] is False:
        # NOT an error exit. Levels above the knee are SUPPOSED to miss their
        # set point - that is the measurement. The orchestrator decides, via
        # --strict-setpoint, and records exceeded_capacity either way.
        print(f"[loadgen] NOTE: closed loop held {sm} MB/s against a target of "
              f"{args.target_wal_mbps} MB/s "
              f"({summary['setpoint_error_frac']*100:+.0f}%) - see on_target")
    return 0


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
