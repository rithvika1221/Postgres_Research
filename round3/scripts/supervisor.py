#!/usr/bin/env python3
"""
Round 3 campaign supervisor - runs the entire study unattended.

Start it, close the laptop, come back a day later. It:

  * starts the publisher monitor in follow mode and keeps it alive
  * publishes campaign_state.json, which the subscriber monitor follows so its
    output files rotate in step without anyone touching that machine
  * runs a health gate before every run (replication live, disk free, database
    responsive) and waits out transient problems instead of failing on them
  * gives every run a wall-clock cap so nothing can hang the campaign
  * retries a failed run, then either skips its family or halts, by policy
  * attempts ONE automatic recovery of a dead subscription before giving up
  * halts hard on conditions that would damage the testbed - a lost replication
    slot, a nearly full data volume
  * checkpoints after every run, so --resume continues where it stopped
  * leaves the system parked safely whatever happens: load stopped, phase idle,
    monitor stopped, state written

Nothing in it ever asks a question.

Exit codes
----------
  0  campaign completed (possibly with skipped families - check the report)
  2  bad configuration
  8  halted on a safety condition

Usage
-----
  set R3_PUB_DSN=host=localhost dbname=pub user=postgres password=...
  set R3_SUB_DSN=host=... dbname=sub user=postgres password=...

  py supervisor.py --campaign campaign.json
  py supervisor.py --campaign campaign.json --resume
  py supervisor.py --campaign campaign.json --dry-run
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone

import psycopg2

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")
DEFAULT_PHASE = os.environ.get("R3_PHASE_FILE", r"C:\r3\phase_state.json")
DEFAULT_STATE = os.environ.get("R3_STATE_FILE", r"C:\r3\campaign_state.json")
DEFAULT_STOP = os.environ.get("R3_STOP_FILE", r"C:\r3\STOP")

HERE = os.path.dirname(os.path.abspath(__file__))

# How often the supervisor looks up from a running orchestrator to refresh the
# heartbeat and confirm the publisher monitor is still alive.
WATCH_INTERVAL = 15.0

# How often the supervisor prints a one-line progress summary while a run is
# in progress. Ten minutes keeps an 18-hour log readable - about 110 lines -
# while never leaving you wondering whether anything is still moving.
PROGRESS_INTERVAL = 600.0


def now():
    return datetime.now(timezone.utc).isoformat()


class Log:
    def __init__(self, path):
        self.path = path
        self.fh = open(path, "a", encoding="utf-8")

    def __call__(self, msg, level="INFO"):
        line = f"{now()}\t{level}\t{msg}"
        print(line if level != "INFO" else msg, flush=True)
        try:
            self.fh.write(line + "\n")
            self.fh.flush()
        except Exception:
            pass

    def close(self):
        try:
            self.fh.close()
        except Exception:
            pass


# --------------------------------------------------------------------------

def write_json_atomic(path, obj):
    """Write JSON so no reader ever sees half a file - and never raise.

    On Windows os.replace() fails with PermissionError while ANY other
    process holds the destination open, even just for reading. Both monitors
    poll campaign_state.json and phase_state.json once a second - the
    subscriber's over SMB, which is stricter still - so the rename collides
    routinely. On Linux the same rename is legal, which is why this never
    showed up in testing.

    A failure here is housekeeping, not experiment data, so it retries
    briefly, falls back to writing in place, and reports success as a bool
    rather than throwing into the caller's error handling.
    """
    d = os.path.dirname(path)
    if d:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            return False
    blob = json.dumps(obj, indent=2, default=str)

    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(blob)
    except Exception:
        tmp = None

    if tmp:
        for attempt in range(5):
            try:
                os.replace(tmp, path)
                return True
            except Exception:
                time.sleep(0.2 * (attempt + 1))
        try:
            os.remove(tmp)
        except Exception:
            pass

    # Last resort: write in place. A reader could catch a partial file, but
    # every reader of these files already tolerates that - read_json returns
    # None rather than raising, and the next sample gets a whole one.
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(blob)
        return True
    except Exception:
        return False


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _kill_tree(proc, log):
    """Kill a process AND its descendants.

    The orchestrator launches loadgen.py, which launches worker processes.
    Terminating only the orchestrator left those workers writing to the
    publisher for the rest of the campaign, so every following level ran under
    a phantom load and no level could reach a truly drained start.
    """
    kids = []
    try:
        import psutil
        kids = psutil.Process(proc.pid).children(recursive=True)
    except Exception:
        pass
    for t in kids:
        try:
            t.terminate()
        except Exception:
            pass
    try:
        proc.terminate()
        proc.wait(timeout=60)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    left = []
    for t in kids:
        try:
            if t.is_running():
                t.kill()
                left.append(t.pid)
        except Exception:
            pass
    if left:
        log(f"  force-killed leftover load generator processes {left}", "WARN")


def free_gb(path):
    try:
        return shutil.disk_usage(path).free / 1024**3
    except Exception:
        return None


class Health:
    """All the checks that decide whether it is safe to start the next run."""

    def __init__(self, pub_dsn, sub_dsn, log):
        self.pub_dsn, self.sub_dsn, self.log = pub_dsn, sub_dsn, log

    def _q(self, dsn, sql):
        try:
            c = psycopg2.connect(dsn, connect_timeout=10)
            c.set_session(autocommit=True)
            k = c.cursor()
            k.execute(sql)
            r = k.fetchone()
            k.close()
            c.close()
            return r
        except Exception as exc:
            self.log(f"health query failed: {type(exc).__name__}: {str(exc)[:160]}", "WARN")
            return None

    def check(self, data_volume, min_free_gb):
        """Return (ok, fatal, reasons)."""
        reasons, fatal = [], False

        r = self._q(self.pub_dsn, "SELECT current_setting('server_version_num')")
        if not r:
            reasons.append("publisher not responding")
        else:
            if int(r[0]) // 10000 != 18:
                reasons.append(f"publisher is PostgreSQL {int(r[0])//10000}, expected 18")
                fatal = True

        if self.sub_dsn:
            if not self._q(self.sub_dsn, "SELECT 1"):
                reasons.append("subscriber not responding")

        r = self._q(self.pub_dsn,
                    "SELECT count(*) FROM pg_stat_replication WHERE application_name='mysub'")
        if r is not None and r[0] == 0:
            reasons.append("no active replication connection")

        r = self._q(self.pub_dsn,
                    "SELECT wal_status FROM pg_replication_slots WHERE slot_type='logical' LIMIT 1")
        if r and r[0] in ("lost", "unreserved"):
            reasons.append(f"replication slot wal_status={r[0]}")
            fatal = True

        fg = free_gb(data_volume)
        if fg is not None:
            if fg < min_free_gb * 0.4:
                reasons.append(f"only {fg:.1f} GB free on {data_volume} - critical")
                fatal = True
            elif fg < min_free_gb:
                reasons.append(f"only {fg:.1f} GB free on {data_volume}")

        return (not reasons), fatal, reasons

    def recover_subscription(self):
        """One attempt to revive a dead subscription. Returns True if it worked."""
        if not self.sub_dsn:
            return False
        self.log("attempting subscription recovery (disable/enable)", "WARN")
        try:
            c = psycopg2.connect(self.sub_dsn, connect_timeout=15)
            c.set_session(autocommit=True)
            k = c.cursor()
            k.execute("ALTER SUBSCRIPTION mysub DISABLE")
            time.sleep(5)
            k.execute("ALTER SUBSCRIPTION mysub ENABLE")
            k.close()
            c.close()
        except Exception as exc:
            self.log(f"recovery failed: {type(exc).__name__}: {str(exc)[:160]}", "ERROR")
            return False
        for _ in range(24):     # up to 2 minutes to reappear
            time.sleep(5)
            r = self._q(self.pub_dsn,
                        "SELECT count(*) FROM pg_stat_replication WHERE application_name='mysub'")
            if r and r[0] > 0:
                self.log("subscription recovered", "WARN")
                return True
        self.log("subscription did not come back", "ERROR")
        return False


# --------------------------------------------------------------------------

class Supervisor:
    def __init__(self, args, campaign):
        self.a = args
        self.c = campaign
        os.makedirs(args.out, exist_ok=True)
        self.log = Log(os.path.join(args.out, "supervisor.log"))
        self.health = Health(args.pub_dsn, args.sub_dsn, self.log)
        self.monitor_proc = None
        self.orch_proc = None
        self._monitor_started_once = False
        self.state = {
            "campaign": campaign.get("name", "round3"),
            "host": socket.gethostname(),
            "started_utc": now(),
            "heartbeat_utc": now(),
            "status": "starting",
            "current_run_id": "pending",
            "current_family": None,
            "completed": [],
            "failed": [],
            "skipped": [],
            "halt_reason": None,
        }
        self.t0 = time.time()
        self._planned = None

    # ---- state ----------------------------------------------------------
    @property
    def checkpoint_path(self):
        """Per-campaign checkpoint.

        campaign_state.json is a LIVE status file that every campaign
        overwrites - it is what the subscriber monitor follows. Resume must not
        read it: running calibration, then the full campaign, then latency all
        share that one path, so --resume on the full campaign would otherwise
        read the latency campaign's progress and re-run everything.
        """
        safe = "".join(c if (c.isalnum() or c in "-_") else "_"
                       for c in str(self.c.get("name", "round3")))
        return os.path.join(self.a.out, f"checkpoint_{safe}.json")

    def save(self, **kw):
        self.state.update(kw)
        self.state["heartbeat_utc"] = now()
        self.state["elapsed_hours"] = round((time.time() - self.t0) / 3600, 3)
        write_json_atomic(self.a.state_file, self.state)
        write_json_atomic(os.path.join(self.a.out, "campaign_state.json"), self.state)
        write_json_atomic(self.checkpoint_path, self.state)

    def load_previous(self):
        prev = read_json(self.checkpoint_path)
        if not prev:
            # Fall back to the live state file, but only if the SAME campaign
            # wrote it. Anything else is another campaign's progress.
            other = read_json(self.a.state_file)
            if other and other.get("campaign") == self.c.get("name"):
                prev = other
                self.log(f"no checkpoint found; resuming from {self.a.state_file}", "WARN")
        if not prev:
            self.log("--resume given but no previous state for this campaign; "
                     "starting fresh", "WARN")
            return set()
        done = {r["run_id"] for r in prev.get("completed", [])}
        self.state["completed"] = prev.get("completed", [])
        self.state["failed"] = prev.get("failed", [])
        self.state["skipped"] = prev.get("skipped", [])
        self.log(f"resuming - {len(done)} runs already completed")
        return done

    # ---- monitor --------------------------------------------------------
    def start_monitor(self):
        if self.a.no_monitor:
            self.log("publisher monitor NOT started (--no-monitor)", "WARN")
            return
        # Clear the stop file only when the campaign first starts. On a
        # mid-campaign restart the stop file may be there because the OPERATOR
        # put it there; deleting it would quietly override a deliberate stop.
        if not self._monitor_started_once:
            try:
                os.remove(self.a.stop_file)
            except Exception:
                pass
            self._monitor_started_once = True
        cmd = [sys.executable, os.path.join(HERE, "monitor.py"),
               "--role", "publisher", "--dsn", self.a.pub_dsn,
               "--out", self.a.out, "--follow",
               "--state-file", self.a.state_file,
               "--phase-file", self.a.phase_file,
               "--stop-file", self.a.stop_file,
               "--data-volume", self.a.data_volume,
               "--disk-low-gb", str(self.a.disk_low_gb)]
        logp = open(os.path.join(self.a.out, "monitor_publisher_stdout.log"), "a",
                    encoding="utf-8")
        self.monitor_proc = subprocess.Popen(cmd, stdout=logp, stderr=subprocess.STDOUT)
        # The monitor refuses to start beside another of its role (exit 4).
        # That is the right answer, but it means this can fail immediately, and
        # ensure_monitor calls it every 15 s - so say it once, not 240 times an
        # hour. A refusal is not an error: it means a monitor IS running, just
        # not one this supervisor owns.
        time.sleep(2.0)
        rc = self.monitor_proc.poll()
        if rc == 4:
            self.monitor_proc = None
            now = time.time()
            if now - getattr(self, "_dup_logged_at", 0) > 600:
                self._dup_logged_at = now
                self.log("a publisher monitor is already connected that this "
                         "supervisor did not start, so a second one was not "
                         "launched. If it is a leftover, stop it and the "
                         "supervisor will take over within a minute.", "WARN")
            return
        self.log(f"publisher monitor started (pid {self.monitor_proc.pid})")

    def monitor_alive(self):
        return self.monitor_proc is not None and self.monitor_proc.poll() is None

    def ensure_monitor(self):
        if self.a.no_monitor:
            return
        if not self.monitor_alive():
            self.log("publisher monitor is not running - restarting it", "WARN")
            self.start_monitor()

    def stop_monitor(self):
        if self.monitor_proc is None:
            return
        # The monitor is stopped by creating the stop file. If the operator did
        # not put it there, we must take it away again: a stop file left behind
        # by a finished campaign makes the NEXT campaign - or a --resume - abort
        # instantly with OPERATOR_STOP, which looks like a broken script.
        ours = not os.path.exists(self.a.stop_file)
        try:
            with open(self.a.stop_file, "w") as fh:
                fh.write(now())
        except Exception:
            ours = False
        try:
            self.monitor_proc.wait(timeout=30)
        except Exception:
            try:
                self.monitor_proc.terminate()
            except Exception:
                pass
        if ours:
            try:
                os.remove(self.a.stop_file)
            except Exception:
                self.log(f"could not remove {self.a.stop_file} - delete it before "
                         f"the next campaign", "WARN")
        self.log("publisher monitor stopped")

    # ---- health gate ----------------------------------------------------
    def gate(self):
        """Wait for the testbed to be healthy. Returns (ok, halt_reason)."""
        deadline = time.time() + self.a.health_wait
        tried_recovery = False
        while True:
            ok, fatal, reasons = self.health.check(self.a.data_volume, self.a.min_free_gb)
            if ok:
                return True, None
            if fatal:
                return False, "; ".join(reasons)
            self.log(f"health gate: {'; '.join(reasons)}", "WARN")

            if (not tried_recovery
                    and any("replication" in r for r in reasons)
                    and self.a.auto_recover):
                tried_recovery = True
                if self.health.recover_subscription():
                    continue

            if time.time() > deadline:
                return False, f"health gate timed out after {self.a.health_wait:.0f}s: " \
                              f"{'; '.join(reasons)}"
            time.sleep(20)

    def log_progress(self, run_id, run_started):
        """One line, every PROGRESS_INTERVAL, while a run is in progress.

        Without this the supervisor log goes silent for the whole length of a
        run - up to four hours - and there is no way to tell a healthy long
        level from a stuck one without opening another file.
        """
        done = len(self.state["completed"])
        failed = len(self.state["failed"])
        skipped = len(self.state["skipped"])
        total = self._planned or (done + failed + skipped + 1)
        elapsed = time.time() - self.t0

        eta = ""
        if done:
            per = sum(r.get("elapsed_sec", 0) for r in self.state["completed"]) / done
            left = total - done - failed - skipped
            if left > 0 and per > 0:
                eta = f" | ~{left * per / 3600:.1f} h left"

        ph = read_json(self.a.phase_file) or {}
        where = ph.get("phase_state", "?")
        if ph.get("level_id") and ph["level_id"] != "idle":
            where = f"{ph['level_id']} {where}"

        self.log(f"  [progress] run {done + 1}/{total} {run_id} | {where} | "
                 f"{(time.time() - run_started) / 60:.0f} min into run | "
                 f"{elapsed / 3600:.1f} h elapsed{eta}")

    # ---- one run --------------------------------------------------------
    def run_once(self, fam, rep, seed, run_id, attempt):
        cfg = os.path.join(HERE, fam["config"])
        # A retry writes under its own run id. Sharing the failed attempt's id
        # would append this attempt's samples to the failed attempt's CSV and
        # event log, so the validator would report the first attempt's failure
        # against the second attempt's data and the two would be averaged
        # together in analysis.
        suffix = "" if attempt == 1 else f"_retry{attempt - 1}"
        artefact_id = run_id + suffix
        if suffix:
            self.log(f"  retry artefacts will be written as {artefact_id}")
            self.save(current_run_id=artefact_id, current_family=fam["family"])
            self.ensure_monitor()
            time.sleep(3)      # let the monitor rotate onto the new run id
        cmd = [sys.executable, os.path.join(HERE, "orchestrate.py"),
               "--config", cfg, "--repeat", str(rep), "--seed", str(seed),
               "--pub-dsn", self.a.pub_dsn, "--out", self.a.out,
               "--phase-file", self.a.phase_file,
               "--drain-timeout", str(self.a.drain_timeout),
               "--min-free-gb", str(self.a.min_free_gb),
               "--data-volume", self.a.data_volume,
               "--run-suffix", suffix,
               "--on-error", "abort"]
        if self.a.sub_dsn:
            cmd += ["--sub-dsn", self.a.sub_dsn]
        if fam.get("no_randomise"):
            cmd.append("--no-randomise")
        if fam.get("strict_setpoint"):
            cmd.append("--strict-setpoint")
        if fam.get("latency_gate"):
            # Family F is attended: the orchestrator will block before each
            # level until the operator confirms the clumsy setting. The run cap
            # for such a family has to cover human pauses, not just work.
            cmd.append("--latency-gate")

        cap = fam.get("max_run_seconds", self.a.max_run_seconds)
        self.log(f"  launching orchestrator (attempt {attempt}, cap {cap/3600:.1f} h)")
        t = time.time()
        # Watch the orchestrator instead of blocking on it. A single
        # p.wait(timeout=cap) is silent for as long as the run lasts - up to
        # four hours - so the heartbeat in campaign_state.json goes stale and a
        # dead monitor is not noticed until the run ends. Polling costs
        # nothing and makes both visible within WATCH_INTERVAL seconds.
        p = None
        try:
            p = subprocess.Popen(cmd)
            # Held on the instance so the signal handler can reach it. It used
            # to be a local, so a SIGTERM here parked the testbed, stopped the
            # monitor, exited - and left the orchestrator running with its
            # sixteen load generators still writing to the publisher. The
            # loadgen tree is deliberately in its own process group so it can
            # be killed as a unit, which also means it does NOT die with its
            # parent. Seen on the rig: eighteen loadgen processes outlived
            # every part of the harness that knew about them.
            self.orch_proc = p
            deadline = t + cap
            next_progress = time.time() + PROGRESS_INTERVAL
            rc = None
            while True:
                try:
                    rc = p.wait(timeout=WATCH_INTERVAL)
                    break
                except subprocess.TimeoutExpired:
                    pass
                # Housekeeping must never be mistaken for a failed run. This
                # block refreshes the heartbeat, restarts a dead monitor and
                # prints progress; if any of it fails, the ORCHESTRATOR is
                # still running perfectly well and must be allowed to finish.
                try:
                    self.save(current_run_id=artefact_id, current_family=fam["family"])
                    self.ensure_monitor()
                    if time.time() >= next_progress:
                        next_progress = time.time() + PROGRESS_INTERVAL
                        self.log_progress(artefact_id, t)
                except Exception:
                    self.log("  supervisor housekeeping error, the run continues:\n"
                             + traceback.format_exc(), "WARN")
                if time.time() >= deadline:
                    self.log(f"  run exceeded its {cap/3600:.1f} h cap - terminating",
                             "ERROR")
                    _kill_tree(p, self.log)
                    rc = -9
                    break
        except Exception:
            self.log("  supervisor failed while running the orchestrator:\n"
                     + traceback.format_exc(), "ERROR")
            if p is not None:
                try:
                    p.kill()
                except Exception:
                    pass
            rc = -1
        finally:
            self.orch_proc = None
        el = time.time() - t

        val_rc = None
        try:
            v = subprocess.run(
                [sys.executable, os.path.join(HERE, "validate_run.py"),
                 "--run-id", artefact_id, "--out", self.a.out],
                capture_output=True, text=True, timeout=600)
            val_rc = v.returncode
            with open(os.path.join(self.a.out, f"validation_{artefact_id}.txt"),
                      "w", encoding="utf-8") as fh:
                fh.write(v.stdout + "\n" + v.stderr)
        except Exception:
            self.log("  validation could not run", "WARN")

        return {"run_id": run_id, "artefact_run_id": artefact_id,
                "family": fam["family"], "repeat": rep,
                "seed": seed, "attempt": attempt,
                "orchestrator_rc": rc, "validator_rc": val_rc,
                "elapsed_sec": round(el, 1), "finished_utc": now()}

    # ---- campaign -------------------------------------------------------
    def go(self):
        plan = []
        for fam in self.c["families"]:
            for rep in fam["repeats"]:
                seed = fam.get("seeds", {}).get(str(rep)) or (1000 + rep * 1013)
                plan.append((fam, rep, seed, f"{fam['family']}_rep{rep}"))

        self.log("=" * 74)
        self.log(f"CAMPAIGN {self.c.get('name','round3')} - {len(plan)} runs")
        for fam, rep, seed, rid in plan:
            self.log(f"   {rid:<28} seed={seed}")
        self.log(f"budget: {self.a.max_campaign_hours} h   "
                 f"per-run cap: {self.a.max_run_seconds/3600:.1f} h   "
                 f"retries: {self.a.retries}")
        self.log("=" * 74)

        if self.a.dry_run:
            self.log("dry run - nothing executed")
            return 0

        self._planned = len(plan)
        done = self.load_previous() if self.a.resume else set()
        self.save(status="running")
        self.start_monitor()

        halted = None
        skip_families = set()

        for fam, rep, seed, run_id in plan:
            if os.path.exists(self.a.stop_file) and not self.monitor_alive():
                halted = "operator stop file"
                break
            if (time.time() - self.t0) / 3600 > self.a.max_campaign_hours:
                halted = f"campaign budget of {self.a.max_campaign_hours} h exhausted"
                break
            if run_id in done:
                self.log(f"\n>>> {run_id} already complete - skipping")
                continue
            if fam["family"] in skip_families:
                self.log(f"\n>>> {run_id} skipped - family already failed")
                self.state["skipped"].append({"run_id": run_id, "reason": "family failed"})
                self.save()
                continue

            self.log(f"\n{'='*74}\n>>> {run_id}   "
                     f"({(time.time()-self.t0)/3600:.2f} h elapsed)\n{'='*74}")
            self.save(current_run_id=run_id, current_family=fam["family"])
            self.ensure_monitor()
            time.sleep(3)   # let the monitor rotate its file before the run starts

            ok, reason = self.gate()
            if not ok:
                self.log(f"HALT: {reason}", "ERROR")
                halted = reason
                break

            result = None
            for attempt in range(1, self.a.retries + 2):
                result = self.run_once(fam, rep, seed, run_id, attempt)
                if result["orchestrator_rc"] == 0:
                    break
                self.log(f"  attempt {attempt} failed "
                         f"(orchestrator rc={result['orchestrator_rc']})", "WARN")
                if attempt <= self.a.retries:
                    self.log(f"  cooling down {self.a.retry_cooldown:.0f}s before retry", "WARN")
                    time.sleep(self.a.retry_cooldown)
                    ok, reason = self.gate()
                    if not ok:
                        self.log(f"HALT during retry gate: {reason}", "ERROR")
                        halted = reason
                        break
            if halted:
                break

            if result["orchestrator_rc"] == 0:
                self.log(f"  {run_id} COMPLETE in {result['elapsed_sec']/60:.1f} min "
                         f"(validator rc={result['validator_rc']})")
                self.state["completed"].append(result)
            else:
                self.log(f"  {run_id} FAILED after {result['attempt']} attempts", "ERROR")
                self.state["failed"].append(result)
                if self.a.on_family_failure == "halt":
                    halted = f"{run_id} failed and --on-family-failure=halt"
                    self.save()
                    break
                if self.a.on_family_failure == "skip-family":
                    skip_families.add(fam["family"])
                    self.log(f"  skipping the rest of family {fam['family']}", "WARN")
            self.save()

        self.finish(halted)
        return 8 if halted else 0

    # ---- shutdown -------------------------------------------------------
    def park(self):
        """Leave the testbed safe: no load, phase idle, backlog drained."""
        self.log("parking the testbed")
        try:
            write_json_atomic(self.a.phase_file,
                              {"phase_state": "idle", "level_id": "idle",
                               "updated": now()})
        except Exception:
            pass
        try:
            c = psycopg2.connect(self.a.pub_dsn, connect_timeout=10)
            c.set_session(autocommit=True)
            k = c.cursor()
            for _ in range(int(self.a.park_drain_seconds)):
                k.execute("""SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),
                             replay_lsn),0)::bigint FROM pg_stat_replication
                             WHERE application_name='mysub' LIMIT 1""")
                r = k.fetchone()
                if not r or r[0] <= 1_048_576:
                    break
                time.sleep(1)
            k.close()
            c.close()
        except Exception:
            pass

    def finish(self, halted):
        self.park()
        self.stop_monitor()
        self.state["finished_utc"] = now()
        self.state["halt_reason"] = halted
        self.save(status="halted" if halted else "finished", current_run_id="idle")
        self.report()
        el = (time.time() - self.t0) / 3600
        self.log("\n" + "=" * 74)
        self.log(f"CAMPAIGN {'HALTED' if halted else 'FINISHED'} after {el:.2f} h")
        if halted:
            self.log(f"reason: {halted}", "ERROR")
        self.log(f"completed {len(self.state['completed'])}, "
                 f"failed {len(self.state['failed'])}, "
                 f"skipped {len(self.state['skipped'])}")
        self.log(f"report: {os.path.join(self.a.out, 'campaign_report.md')}")
        self.log("=" * 74)
        self.log.close()

    def report(self):
        s = self.state
        L = [f"# Campaign report — {s['campaign']}\n",
             f"- **Host**: {s['host']}",
             f"- **Started**: {s['started_utc']}",
             f"- **Finished**: {s.get('finished_utc','(running)')}",
             f"- **Elapsed**: {s.get('elapsed_hours','?')} h",
             f"- **Status**: {s.get('status')}"]
        if s.get("halt_reason"):
            L.append(f"\n> **Halted**: {s['halt_reason']}\n")
        L += [f"\n**{len(s['completed'])} completed, {len(s['failed'])} failed, "
              f"{len(s['skipped'])} skipped**\n",
              "## Completed\n",
              "| Run | Seed | Attempt | Minutes | Validator |",
              "|---|---|---|---|---|"]
        for r in s["completed"]:
            v = r.get("validator_rc")
            L.append(f"| {r['run_id']} | {r['seed']} | {r['attempt']} | "
                     f"{r['elapsed_sec']/60:.1f} | "
                     f"{'clean' if v == 0 else 'PROBLEMS' if v == 1 else '—'} |")
        if s["failed"]:
            L += ["\n## Failed\n", "| Run | Attempts | Orchestrator rc | Minutes |",
                  "|---|---|---|---|"]
            for r in s["failed"]:
                L.append(f"| {r['run_id']} | {r['attempt']} | "
                         f"{r['orchestrator_rc']} | {r['elapsed_sec']/60:.1f} |")
            L.append("\nRead `run_report_<run>.md` for each failure. Re-run with "
                     "`py supervisor.py --campaign campaign.json --resume`.\n")
        if s["skipped"]:
            L += ["\n## Skipped\n"] + [f"- {r['run_id']} — {r['reason']}"
                                       for r in s["skipped"]]
        L.append("\n## Next steps\n")
        L.append("1. Read every `run_report_*.md` and `validation_*.txt`")
        L.append("2. Copy the whole data directory off both VMs **before** deallocating")
        L.append("3. Re-run any failed family with `--resume`\n")
        with open(os.path.join(self.a.out, "campaign_report.md"), "w",
                  encoding="utf-8") as fh:
            fh.write("\n".join(L))


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Round 3 unattended campaign supervisor.")
    ap.add_argument("--campaign", default=os.path.join(HERE, "campaign.json"))
    ap.add_argument("--pub-dsn", default=None)
    ap.add_argument("--sub-dsn", default=None)
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--phase-file", default=DEFAULT_PHASE)
    ap.add_argument("--state-file", default=DEFAULT_STATE)
    ap.add_argument("--stop-file", default=DEFAULT_STOP)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-monitor", action="store_true",
                    help="do not start the publisher monitor (you started it yourself)")
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--retry-cooldown", type=float, default=120.0)
    ap.add_argument("--on-family-failure", choices=["skip-family", "continue", "halt"],
                    default="skip-family")
    ap.add_argument("--max-run-seconds", type=float, default=4 * 3600)
    ap.add_argument("--max-campaign-hours", type=float, default=26.0)
    ap.add_argument("--drain-timeout", type=float, default=3600.0)
    ap.add_argument("--health-wait", type=float, default=600.0)
    ap.add_argument("--auto-recover", action="store_true", default=True)
    ap.add_argument("--no-auto-recover", dest="auto_recover", action="store_false")
    ap.add_argument("--min-free-gb", type=float, default=50.0)
    ap.add_argument("--data-volume", default="F:\\" if os.name == "nt" else "/")
    ap.add_argument("--disk-low-gb", type=float,
                    default=float(os.environ.get("R3_DISK_LOW_GB", "25")),
                    help="passed to the publisher monitor. Keep it at 25 for "
                         "the real campaign - the subscribers have 1 TB "
                         "volumes and never go near it.")
    ap.add_argument("--park-drain-seconds", type=float, default=600.0)
    args = ap.parse_args()

    args.pub_dsn = args.pub_dsn or os.environ.get("R3_PUB_DSN")
    args.sub_dsn = args.sub_dsn or os.environ.get("R3_SUB_DSN")
    if not args.pub_dsn:
        print("ERROR: no publisher DSN. Pass --pub-dsn or set R3_PUB_DSN.")
        return 2
    if not args.sub_dsn:
        print("WARNING: no R3_SUB_DSN - subscriber health checks and automatic "
              "recovery are disabled.")

    campaign = read_json(args.campaign)
    if not campaign or "families" not in campaign:
        print(f"ERROR: cannot read a campaign from {args.campaign}")
        return 2
    for fam in campaign["families"]:
        p = os.path.join(HERE, fam["config"])
        if not os.path.isfile(p):
            print(f"ERROR: matrix file missing: {p}")
            return 2
    for name in ("orchestrate.py", "validate_run.py", "loadgen.py", "monitor.py"):
        if not os.path.isfile(os.path.join(HERE, name)):
            print(f"ERROR: {name} not found beside supervisor.py")
            return 2

    sup = Supervisor(args, campaign)

    def bail(signum, frame):
        sup.log(f"signal {signum} received - shutting down safely", "WARN")
        orch = getattr(sup, "orch_proc", None)
        if orch is not None and orch.poll() is None:
            sup.log(f"  stopping the orchestrator (pid {orch.pid}) and its "
                    f"load generators", "WARN")
            try:
                _kill_tree(orch, sup.log)
            except Exception:
                try:
                    orch.kill()
                except Exception:
                    pass
        sup.finish("interrupted by signal")
        os._exit(8)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, bail)
        except Exception:
            pass

    try:
        return sup.go()
    except Exception:
        sup.log("UNHANDLED EXCEPTION:\n" + traceback.format_exc(), "ERROR")
        sup.finish("unhandled exception in supervisor")
        return 8


if __name__ == "__main__":
    raise SystemExit(main())
