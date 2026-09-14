#!/usr/bin/env python3
"""
Round 3 monitor.  STANDALONE and SELF-HEALING.

Designed to be started once and left alone for a day. It never exits because
of an error: the database connection is re-established with backoff, queries
that fail are logged and retried, and the output file rotates automatically
when the campaign moves to a new run.

Stopping it:
  * create the stop file (default C:\\r3\\STOP) - the supervisor does this
  * or --duration seconds
  * or Ctrl-C

Outputs, per run id:
  <role>_<run_id>.csv           one row per sample
  <role>_<run_id>_events.log    everything notable that happened

Usage
-----
  # follow the campaign, rotating files as runs change - start once, walk away
  py monitor.py --role publisher --follow

  # subscriber, reading the shared state written by the publisher
  py monitor.py --role subscriber --follow --state-file \\\\R3PUB\\r3\\campaign_state.json

  # fixed run id, no state file at all
  py monitor.py --role publisher --run-id smoke --level-id t1 --duration 300
"""

import argparse
import csv
import io
import json
import os
import socket
import time
import traceback
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

try:
    import psutil
except ImportError:
    psutil = None

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")
DEFAULT_PHASE = os.environ.get("R3_PHASE_FILE", r"C:\r3\phase_state.json")
DEFAULT_STATE = os.environ.get("R3_STATE_FILE", r"C:\r3\campaign_state.json")
DEFAULT_STOP = os.environ.get("R3_STOP_FILE", r"C:\r3\STOP")

GAP_WARN_SEC = 3.0
RECONNECT_MAX_BACKOFF = 60.0


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------

# WAL write/fsync timing moved between releases.
#
# PostgreSQL 18 REMOVED wal_write, wal_sync, wal_write_time and wal_sync_time
# from pg_stat_wal (commits 2421e9a51 / 6c349d83b) and made track_wal_io_timing
# feed pg_stat_io instead. Querying the old columns on 18 raises
# UndefinedColumn on EVERY sample, which silently produces a campaign of empty
# rows. The clause is therefore chosen from the connected server's version, and
# the monitor degrades through the list below rather than failing, so that a
# future catalogue change costs two columns instead of a whole run.
WAL_TIMING_SQL = {
    "pg_stat_io": """
    (SELECT sum(write_time) FROM pg_stat_io WHERE object='wal') AS wal_write_time_ms,
    (SELECT sum(fsync_time) FROM pg_stat_io WHERE object='wal') AS wal_sync_time_ms,""",
    "pg_stat_wal": """
    (SELECT wal_write_time   FROM pg_stat_wal)  AS wal_write_time_ms,
    (SELECT wal_sync_time    FROM pg_stat_wal)  AS wal_sync_time_ms,""",
    "none": """
    NULL::float8 AS wal_write_time_ms,
    NULL::float8 AS wal_sync_time_ms,""",
}

# Preference order per major version. The last entry always works.
# NOTE the pre-18 chain does NOT include pg_stat_io. That view exists on 16
# and 17 with the same column names, but carries no object='wal' rows there,
# so the query would succeed and return NULL for the rest of the campaign
# while the event log claimed the source was working.
WAL_TIMING_ORDER = {
    "ge18": ["pg_stat_io", "none"],
    "lt18": ["pg_stat_wal", "none"],
}

PUB_SQL_TEMPLATE = """
SELECT
    (SELECT wal_records      FROM pg_stat_wal)  AS wal_records,
    (SELECT wal_fpi          FROM pg_stat_wal)  AS wal_fpi,
    (SELECT wal_bytes::bigint FROM pg_stat_wal) AS wal_bytes,
    (SELECT wal_buffers_full FROM pg_stat_wal)  AS wal_buffers_full,
    (SELECT EXTRACT(EPOCH FROM stats_reset) FROM pg_stat_wal) AS wal_stats_reset,
{wal_timing}
    (SELECT xact_commit   FROM pg_stat_database WHERE datname=current_database()) AS xact_commit,
    (SELECT xact_rollback FROM pg_stat_database WHERE datname=current_database()) AS xact_rollback,
    (SELECT tup_inserted  FROM pg_stat_database WHERE datname=current_database()) AS tup_inserted,
    (SELECT tup_updated   FROM pg_stat_database WHERE datname=current_database()) AS tup_updated,
    (SELECT tup_deleted   FROM pg_stat_database WHERE datname=current_database()) AS tup_deleted,
    (SELECT blks_read     FROM pg_stat_database WHERE datname=current_database()) AS blks_read,
    (SELECT blks_hit      FROM pg_stat_database WHERE datname=current_database()) AS blks_hit,
    (SELECT blk_read_time FROM pg_stat_database WHERE datname=current_database()) AS blk_read_time_ms,
    (SELECT blk_write_time FROM pg_stat_database WHERE datname=current_database()) AS blk_write_time_ms,
    pg_current_wal_lsn()::text        AS current_wal_lsn,
    pg_current_wal_insert_lsn()::text AS insert_lsn,
    (SELECT count(*) FROM pg_stat_activity
      WHERE state='active' AND pid <> pg_backend_pid()) AS active_backends
"""


def build_pub_sql(mode):
    return PUB_SQL_TEMPLATE.format(wal_timing=WAL_TIMING_SQL[mode])


PUB_REPL_SQL = """
SELECT application_name, state,
       sent_lsn::text, write_lsn::text, flush_lsn::text, replay_lsn::text,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn),   0)::bigint AS send_lag_bytes,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn),  0)::bigint AS write_lag_bytes,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn),  0)::bigint AS flush_lag_bytes,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0)::bigint AS replay_lag_bytes,
       EXTRACT(EPOCH FROM write_lag)  AS write_lag_sec,
       EXTRACT(EPOCH FROM flush_lag)  AS flush_lag_sec,
       EXTRACT(EPOCH FROM replay_lag) AS replay_lag_sec
FROM pg_stat_replication WHERE application_name='mysub' LIMIT 1
"""

PUB_SLOT_SQL = """
SELECT slot_name, active, wal_status,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint AS retained_wal_bytes,
       COALESCE(safe_wal_size,0)::bigint AS safe_wal_size
FROM pg_replication_slots WHERE slot_type='logical' LIMIT 1
"""

SUB_SQL = """
SELECT
    (SELECT xact_commit    FROM pg_stat_database WHERE datname=current_database()) AS xact_commit,
    (SELECT tup_inserted   FROM pg_stat_database WHERE datname=current_database()) AS tup_inserted,
    (SELECT tup_updated    FROM pg_stat_database WHERE datname=current_database()) AS tup_updated,
    (SELECT tup_deleted    FROM pg_stat_database WHERE datname=current_database()) AS tup_deleted,
    (SELECT blks_read      FROM pg_stat_database WHERE datname=current_database()) AS blks_read,
    (SELECT blks_hit       FROM pg_stat_database WHERE datname=current_database()) AS blks_hit,
    (SELECT blk_write_time FROM pg_stat_database WHERE datname=current_database()) AS blk_write_time_ms,
    (SELECT EXTRACT(EPOCH FROM stats_reset)
       FROM pg_stat_database WHERE datname=current_database()) AS db_stats_reset,
    (SELECT count(*) FROM pg_stat_activity
      WHERE backend_type LIKE '%%apply worker%%' OR application_name LIKE 'mysub%%') AS apply_workers
"""

# The exact row count used to live in SUB_SQL, one full sequential scan of
# ingest_data per sample, once a second. On an empty table that is free, which
# is why it survived every test. On a real one it is ruinous: at 39 million
# rows a single scan took five minutes, so sampling collapsed - and worse, the
# monitor was loading the machine it exists to observe, evicting the apply
# worker's pages and competing for IO, with the interference growing as the
# table grew during a level. That is a confound that scales with the
# independent variable.
#
# rows_applied_per_sec has always been derived from tup_inserted, not from
# this, so apply throughput is unaffected. row_count is a sanity column, and a
# sanity column does not need to be sampled at 1 Hz.
SUB_ROWCOUNT_SQL = "SELECT count(*)::bigint FROM ingest_data"

# How often to pay for it, and how long to allow before giving up on it. The
# timeout matters: without one, a slow count would stall the whole sampler.
ROW_COUNT_INTERVAL_SEC = 30.0
ROW_COUNT_TIMEOUT_MS = 4000

# How often to repeat the warning that the campaign state file cannot be
# read. Ten minutes is often enough to be noticed and rare enough not to
# drown the event log over a day.
STATE_NAG_SEC = 600.0

SUB_STAT_SQL = """
SELECT subname, received_lsn::text, latest_end_lsn::text,
       EXTRACT(EPOCH FROM (now()-last_msg_send_time))    AS msg_send_age_sec,
       EXTRACT(EPOCH FROM (now()-last_msg_receipt_time)) AS msg_receipt_age_sec
FROM pg_stat_subscription
WHERE subname = 'mysub' AND relid IS NULL      -- the leader apply worker, not a
ORDER BY pid NULLS LAST LIMIT 1                -- table-sync worker
"""

COMMON = ["sample_time", "epoch", "run_id", "host", "level_id", "phase_state",
          "clients", "rows_per_commit", "row_bytes", "target_wal_mbps",
          "network_latency_ms"]

PUB_FIELDS = COMMON + [
    "wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
    "wal_stats_reset", "wal_write_time_ms", "wal_sync_time_ms",
    "xact_commit", "xact_rollback", "tup_inserted", "tup_updated", "tup_deleted",
    "blks_read", "blks_hit", "blk_read_time_ms", "blk_write_time_ms",
    "current_wal_lsn", "insert_lsn", "active_backends", "interval_sec",
    "wal_bytes_delta", "wal_bytes_per_sec", "wal_mb_per_sec",
    "commits_delta", "commits_per_sec", "rows_inserted_delta",
    "rows_inserted_per_sec", "rows_updated_per_sec", "rows_deleted_per_sec",
    "lsn_bytes_per_sec", "wal_sync_time_delta_ms",
    "repl_state", "sent_lsn", "write_lsn", "replay_lsn",
    "send_lag_bytes", "write_lag_bytes", "flush_lag_bytes", "replay_lag_bytes",
    "write_lag_sec", "flush_lag_sec", "replay_lag_sec",
    "d_replay_lag_bytes_per_sec",
    "slot_active", "retained_wal_bytes", "wal_status",
    "cpu_pct", "mem_used_pct", "disk_read_bytes_per_sec", "disk_write_bytes_per_sec",
    "disk_read_iops", "disk_write_iops", "net_sent_bytes_per_sec",
    "net_recv_bytes_per_sec", "data_volume_free_gb", "db_connected",
]

SUB_FIELDS = COMMON + [
    "xact_commit", "tup_inserted", "tup_updated", "tup_deleted",
    "blks_read", "blks_hit", "blk_write_time_ms", "db_stats_reset",
    "row_count", "apply_workers",
    "interval_sec", "commits_per_sec", "rows_applied_per_sec", "row_count_delta",
    "received_lsn", "latest_end_lsn", "msg_send_age_sec", "msg_receipt_age_sec",
    "cpu_pct", "mem_used_pct", "disk_read_bytes_per_sec", "disk_write_bytes_per_sec",
    "disk_read_iops", "disk_write_iops", "net_sent_bytes_per_sec",
    "net_recv_bytes_per_sec", "data_volume_free_gb", "db_connected",
]


# --------------------------------------------------------------------------

def safe_name(run_id):
    """Run ids come from a JSON file written by another machine and are used
    as filenames. ':' alone is enough to make open() fail on Windows."""
    cleaned = "".join(c if (c.isalnum() or c in "._-") else "_"
                      for c in str(run_id))[:80]
    return cleaned or "unnamed"


def lsn_to_int(lsn):
    if not lsn:
        return None
    try:
        hi, lo = lsn.split("/")
        return (int(hi, 16) << 32) + int(lo, 16)
    except Exception:
        return None


def read_json(path):
    """Tolerant read - returns None rather than raising, ever."""
    if not path:
        return None
    try:
        with open(path, "r") as fh:
            return json.load(fh)
    except Exception:
        return None


class Events:
    """Append-only event log. Reopens itself if the handle is lost."""

    def __init__(self, path):
        self.path = path
        self.fh = None
        self.counts = {}
        self.total_counts = {}
        self._open()

    def _open(self):
        try:
            self.fh = open(self.path, "a", encoding="utf-8")
        except Exception:
            self.fh = None

    def write(self, kind, msg, echo=True):
        ts = datetime.now(timezone.utc).isoformat()
        line = f"{ts}\t{kind}\t{msg}\n"
        for _ in range(2):
            try:
                if self.fh is None:
                    self._open()
                if self.fh:
                    self.fh.write(line)
                    self.fh.flush()
                break
            except Exception:
                self.fh = None
        self.counts[kind] = self.counts.get(kind, 0) + 1
        self.total_counts[kind] = self.total_counts.get(kind, 0) + 1
        if echo:
            print(f"  [{kind}] {msg}", flush=True)

    def rotate(self, path):
        try:
            if self.fh:
                self.fh.close()
        except Exception:
            pass
        self.path = path
        self.counts = {}
        self._open()

    def close(self):
        try:
            if self.fh:
                self.fh.close()
        except Exception:
            pass


class DB:
    """Connection that reconnects itself, forever, with backoff."""

    def __init__(self, dsn, events, role="unknown"):
        self.dsn = dsn
        self.role = role
        self.events = events
        self.conn = None
        self.cur = None
        self.backoff = 1.0
        self.connected = False
        self.server_version = None
        self.major = None
        self.timing_modes = None      # remaining WAL-timing candidates
        self.pub_sql = None
        self._seen_errors = set()
        self.last_error_kind = None

    def _select_timing_mode(self):
        """Pick the WAL write/sync timing clause for this server version."""
        if self.timing_modes is None:
            key = "ge18" if (self.major or 0) >= 18 else "lt18"
            self.timing_modes = list(WAL_TIMING_ORDER[key])
            self.events.write("WAL_TIMING_MODE",
                              f"PostgreSQL major {self.major}: using "
                              f"{self.timing_modes[0]} for WAL write/sync timing")
        self.pub_sql = build_pub_sql(self.timing_modes[0])

    def degrade_timing(self, why):
        """The current WAL-timing clause is not supported here. Step down."""
        if not self.timing_modes or len(self.timing_modes) == 1:
            return False
        dropped = self.timing_modes.pop(0)
        self.pub_sql = build_pub_sql(self.timing_modes[0])
        self.events.write("QUERY_DEGRADED",
                          f"WAL timing via {dropped} unsupported ({why}); "
                          f"falling back to {self.timing_modes[0]}. "
                          f"Every other column is unaffected.")
        return True

    def ensure(self):
        if self.conn is not None:
            return True
        try:
            # Tag the connection so anyone can tell from SQL alone whether a
            # monitor is actually running - including from the OTHER machine.
            # Forgetting to start the subscriber monitor is the easiest way to
            # lose half a campaign's data, and it is otherwise invisible from
            # the publisher.
            self.conn = psycopg2.connect(
                self.dsn, connect_timeout=10,
                application_name=f"r3_monitor_{self.role}")
            self.conn.set_session(autocommit=True)
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute("SELECT current_setting('server_version') AS v, "
                             "current_setting('server_version_num') AS n")
            r = self.cur.fetchone()
            self.server_version = r["v"]
            self.major = int(r["n"]) // 10000
            self._select_timing_mode()
            if not self.connected:
                self.events.write("DB_CONNECTED",
                                  f"PostgreSQL {self.server_version}")
            self.connected = True
            self.backoff = 1.0
            return True
        except Exception as exc:
            self.close()
            self.events.write("DB_CONNECT_FAILED",
                              f"{type(exc).__name__}: {str(exc)[:200]} "
                              f"(retry in {self.backoff:.0f}s)")
            time.sleep(self.backoff)
            self.backoff = min(RECONNECT_MAX_BACKOFF, self.backoff * 2)
            return False

    def query(self, sql, tag="query"):
        """Returns a dict, or None. Never raises.

        A ProgrammingError (missing column, missing view, syntax) is NOT a
        broken connection: dropping and redialling the socket would just
        repeat it once a second for the whole campaign. Those are logged once
        per distinct message and left to the caller to handle. Only genuine
        connection faults reconnect.
        """
        if not self.ensure():
            self.last_error_kind = "connect"
            return None
        self.last_error_kind = None
        try:
            self.cur.execute(sql)
            r = self.cur.fetchone()
            return dict(r) if r else {}
        except psycopg2.ProgrammingError as exc:
            self.last_error_kind = "unsupported"
            msg = f"{type(exc).__name__}: {str(exc)[:200]}"
            sig = (tag, msg[:120])
            if sig not in self._seen_errors:
                self._seen_errors.add(sig)
                self.events.write("QUERY_UNSUPPORTED", f"[{tag}] {msg}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return None
        except Exception as exc:
            self.last_error_kind = "connection"
            self.events.write("QUERY_ERROR", f"[{tag}] {type(exc).__name__}: {str(exc)[:200]}")
            self.connected = False
            self.close()
            return None

    def query_scalar_bounded(self, sql, tag="query", timeout_ms=4000):
        """One scalar value, with a hard time limit. Never raises.

        A timeout here is not a broken connection and not an unsupported
        query - it means 'too expensive right now'. Reconnecting would be
        wrong and would repeat the cost. The caller leaves the column empty
        for this sample and carries on.
        """
        if not self.ensure():
            self.last_error_kind = "connect"
            return None
        self.last_error_kind = None
        try:
            self.cur.execute(f"SET statement_timeout = {int(timeout_ms)}")
            try:
                self.cur.execute(sql)
                r = self.cur.fetchone()
                if not r:
                    return None
                return list(r.values())[0]
            except psycopg2.errors.QueryCanceled:
                self.last_error_kind = "timeout"
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                sig = (tag, "timeout")
                if sig not in self._seen_errors:
                    self._seen_errors.add(sig)
                    self.events.write(
                        "QUERY_TOO_SLOW",
                        f"[{tag}] did not finish within {timeout_ms} ms. That "
                        f"column is left empty; nothing else is affected. "
                        f"Logged once.")
                return None
            finally:
                try:
                    self.cur.execute("SET statement_timeout = 0")
                except Exception:
                    pass
        except psycopg2.ProgrammingError as exc:
            self.last_error_kind = "unsupported"
            msg = f"{type(exc).__name__}: {str(exc)[:200]}"
            sig = (tag, msg[:120])
            if sig not in self._seen_errors:
                self._seen_errors.add(sig)
                self.events.write("QUERY_UNSUPPORTED", f"[{tag}] {msg}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return None
        except Exception as exc:
            self.last_error_kind = "connection"
            self.events.write("QUERY_ERROR",
                              f"[{tag}] {type(exc).__name__}: {str(exc)[:200]}")
            self.connected = False
            self.close()
            return None

    def close(self):
        for o in (self.cur, self.conn):
            try:
                if o:
                    o.close()
            except Exception:
                pass
        self.cur = self.conn = None


def host_rates(prev, now_t, volume):
    out = {k: None for k in ("cpu_pct", "mem_used_pct", "disk_read_bytes_per_sec",
                             "disk_write_bytes_per_sec", "disk_read_iops",
                             "disk_write_iops", "net_sent_bytes_per_sec",
                             "net_recv_bytes_per_sec", "data_volume_free_gb")}
    if psutil is None:
        return out, None
    try:
        d, n = psutil.disk_io_counters(), psutil.net_io_counters()
        cur = {"t": now_t, "d": d, "n": n}
        out["cpu_pct"] = psutil.cpu_percent(interval=None)
        out["mem_used_pct"] = psutil.virtual_memory().percent
        try:
            out["data_volume_free_gb"] = round(psutil.disk_usage(volume).free / 1024**3, 2)
        except Exception:
            pass
        if prev:
            dt = now_t - prev["t"]
            if dt > 0:
                pd_, pn = prev["d"], prev["n"]
                out["disk_read_bytes_per_sec"] = (d.read_bytes - pd_.read_bytes) / dt
                out["disk_write_bytes_per_sec"] = (d.write_bytes - pd_.write_bytes) / dt
                out["disk_read_iops"] = (d.read_count - pd_.read_count) / dt
                out["disk_write_iops"] = (d.write_count - pd_.write_count) / dt
                out["net_sent_bytes_per_sec"] = (n.bytes_sent - pn.bytes_sent) / dt
                out["net_recv_bytes_per_sec"] = (n.bytes_recv - pn.bytes_recv) / dt
        return out, cur
    except Exception:
        return out, prev


class Writer:
    """CSV writer that rotates when the run id changes."""

    def __init__(self, out_dir, role, fields, events):
        self.out, self.role, self.fields = out_dir, role, fields
        self.events = events
        self.run_id = None
        self.fh = self.w = None
        self.n = 0

    @staticmethod
    def _header_of(path):
        """The column names already in a CSV, or None if unreadable."""
        try:
            with open(path, newline="", encoding="utf-8-sig") as fh:
                first = fh.readline()
            if not first.strip():
                return None
            return next(csv.reader(io.StringIO(first)))
        except Exception:
            return None

    def switch(self, run_id):
        if run_id == self.run_id:
            return
        self.close()
        self.run_id = run_id
        path = os.path.join(self.out, f"{self.role}_{run_id}.csv")

        # Appending to an existing file is deliberate - a monitor restart must
        # not lose the samples already written. But appending rows that do not
        # match the header already in that file is silent corruption: when
        # db_stats_reset was added to the schema, a later monitor wrote 59,698
        # 39-column rows underneath a 38-column header from three weeks
        # earlier, and every one of them reads as malformed unless you know to
        # match on field count. If the header does not match, start a new file
        # rather than writing rows the header cannot describe.
        exists = os.path.isfile(path)
        if exists:
            old = self._header_of(path)
            if old is not None and old != list(self.fields):
                base, ext = os.path.splitext(path)
                n = 2
                while os.path.isfile(f"{base}_v{n}{ext}"):
                    n += 1
                newpath = f"{base}_v{n}{ext}"
                self.events.write(
                    "SCHEMA_CHANGED",
                    f"{os.path.basename(path)} has {len(old)} columns, this "
                    f"monitor writes {len(self.fields)}. Writing to "
                    f"{os.path.basename(newpath)} instead of appending rows "
                    f"its header cannot describe.")
                path = newpath
                exists = False

        self.fh = open(path, "a", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.fh, fieldnames=self.fields, extrasaction="ignore")
        if not exists:
            self.w.writeheader()
        self.n = 0
        self.events.rotate(os.path.join(self.out, f"{self.role}_{run_id}_events.log"))
        self.events.write("RUN", f"now recording run_id={run_id} -> {path}")

    def row(self, r):
        try:
            self.w.writerow(r)
            self.fh.flush()
            self.n += 1
        except Exception as exc:
            self.events.write("WRITE_ERROR", f"{type(exc).__name__}: {exc}")

    def close(self):
        try:
            if self.fh:
                self.fh.close()
        except Exception:
            pass
        self.fh = self.w = None


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Round 3 self-healing monitor.")
    ap.add_argument("--role", required=True, choices=["publisher", "subscriber"])
    ap.add_argument("--dsn", default=None, help="defaults to $R3_PUB_DSN / $R3_SUB_DSN")
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--run-id", default=None,
                    help="fixed run id; omit with --follow to track the campaign")
    ap.add_argument("--follow", action="store_true",
                    help="read the current run id from --state-file and rotate files")
    ap.add_argument("--state-file", default=DEFAULT_STATE)
    ap.add_argument("--phase-file", default=DEFAULT_PHASE)
    ap.add_argument("--no-phase-file", action="store_true")
    ap.add_argument("--level-id", default="adhoc")
    ap.add_argument("--stop-file", default=DEFAULT_STOP,
                    help="exit cleanly when this file appears")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--allow-duplicate", action="store_true",
                    help="start even if another monitor of this role is already "
                         "connected. Almost never what you want: two monitors "
                         "append to the same CSV.")
    ap.add_argument("--row-count-interval", type=float,
                    default=ROW_COUNT_INTERVAL_SEC,
                    help="subscriber only: seconds between exact row counts of "
                         "ingest_data. Each one is a sequential scan, so this "
                         "must never run at the sample rate. 0 disables it; "
                         "rows_applied_per_sec does not depend on it.")
    ap.add_argument("--duration", type=float, default=0.0, help="0 = run until stopped")
    ap.add_argument("--data-volume", default="F:\\" if os.name == "nt" else "/")
    # This floor used to be the bare literal 25 in the sampling loop.
    # validate_run.py counts a DISK_LOW event as a hard PROBLEM, so one dip
    # below it fails every run of a campaign - and on a machine that simply
    # has less disk than the testbed, that is unrelated to the experiment.
    # Making it an argument means the threshold a run was judged against is
    # a recorded choice instead of a constant nobody can see.
    ap.add_argument("--disk-low-gb", type=float,
                    default=float(os.environ.get("R3_DISK_LOW_GB", "25")),
                    help="raise a DISK_LOW event below this many GB free on "
                         "--data-volume. validate_run.py treats DISK_LOW as a "
                         "hard failure, so lower it only when the volume is "
                         "genuinely smaller than the testbed's, and say so in "
                         "the run log.")
    args = ap.parse_args()

    dsn = args.dsn or os.environ.get(
        "R3_PUB_DSN" if args.role == "publisher" else "R3_SUB_DSN")
    if not dsn:
        env = "R3_PUB_DSN" if args.role == "publisher" else "R3_SUB_DSN"
        print(f"ERROR: no DSN. Pass --dsn or set {env}.")
        return 2
    if not args.run_id and not args.follow:
        print("ERROR: pass --run-id, or --follow to track the campaign.")
        return 2

    os.makedirs(args.out, exist_ok=True)
    fields = PUB_FIELDS if args.role == "publisher" else SUB_FIELDS
    host = socket.gethostname()

    events = Events(os.path.join(args.out, f"{args.role}_startup_events.log"))
    events.write("START", f"role={args.role} host={host} follow={args.follow} "
                          f"interval={args.interval}s stop_file={args.stop_file}")
    if psutil is None:
        events.write("WARNING", "psutil not installed - no host counters")

    db = DB(dsn, events, args.role)

    # Refuse to be the second monitor. There are several ways to start this -
    # by hand in an RDP window, from start_subscriber_monitor.ps1, and as the
    # R3SubscriberMonitor scheduled task - and none of them knew about the
    # others. Three ended up running at once, all appending to the same CSV,
    # all loading the machine they exist to observe. The connection tag makes
    # the check a single query, and this is the one place every launch path
    # goes through.
    if not args.allow_duplicate:
        tag = f"r3_monitor_{args.role}"
        r = db.query(
            "SELECT count(*)::int AS n FROM pg_stat_activity "
            f"WHERE application_name = '{tag}' AND pid <> pg_backend_pid()",
            tag="duplicate_check")
        n = (r or {}).get("n")
        if n:
            msg = (f"another {args.role} monitor is already connected "
                   f"({n} session(s) tagged {tag}). Refusing to start a second "
                   f"one: they would interleave rows in the same CSV.")
            events.write("DUPLICATE_MONITOR", msg)
            print(f"[monitor] {msg}\n")
            print("  Stop the other one first:")
            print(f"    Get-ScheduledTask -TaskName R3SubscriberMonitor -EA SilentlyContinue |"
                  f" Unregister-ScheduledTask -Confirm:$false")
            print("    Get-CimInstance Win32_Process -Filter \"Name like '%python%'\" |")
            print("      Where-Object { $_.CommandLine -like '*monitor.py*' } |")
            print("      ForEach-Object { Stop-Process -Id $_.ProcessId -Force }")
            print("  then, on the database, clear any backend left mid-query:")
            print(f"    SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                  f" WHERE application_name = '{tag}';")
            print("\n  Use --allow-duplicate only if you genuinely want two.")
            return 4

    writer = Writer(args.out, args.role, fields, events)
    writer.switch(args.run_id or "pending")

    prev = prev_host = None
    last_level = last_status = last_state = None
    # The exact subscriber row count is taken on a slow cadence of its own, so
    # its delta must be measured against the last sample that actually carried
    # one - not against the previous sample, which usually will not have.
    next_row_count = 0.0
    last_rc = None                    # (epoch, value)
    unreadable_since = None           # state file, --follow only
    t_begin = time.time()
    n_total = 0
    fatal = False
    last_switch_fail = None
    if psutil:
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

    print(f"[monitor] {args.role} on {host} - running until stopped")
    print(f"[monitor] stop with:  echo. > {args.stop_file}\n")

    try:
        while True:
            t0 = time.time()

            if os.path.exists(args.stop_file):
                events.write("STOP", "stop file present")
                break
            if args.duration and (t0 - t_begin) >= args.duration:
                events.write("STOP", f"duration {args.duration}s reached")
                break

            # ---- which run / level are we in? -----------------------------
            run_id = args.run_id
            if args.follow:
                st = read_json(args.state_file)
                if st and st.get("current_run_id"):
                    run_id = st["current_run_id"]
                    unreadable_since = None
                elif writer.run_id and writer.run_id != "pending":
                    run_id = writer.run_id      # keep the last known good id
                else:
                    run_id = "pending"
                    # Everything still records correctly - but under one file
                    # called 'pending' for the whole campaign, with no run or
                    # level labels. A whole campaign was collected that way
                    # because this was silent. Say so, on a slow repeat, so a
                    # glance at the window or the event log catches it.
                    if unreadable_since is None:
                        unreadable_since = t0
                        events.write(
                            "STATE_FILE_UNREADABLE",
                            f"cannot read {args.state_file} - recording as "
                            f"'pending' with no run or level labels. On the "
                            f"PUBLISHER run:  New-SmbShare -Name r3 -Path "
                            f"C:\\r3 -ReadAccess Everyone")
                    elif t0 - unreadable_since >= STATE_NAG_SEC:
                        unreadable_since = t0
                        events.write(
                            "STATE_FILE_UNREADABLE",
                            f"still unreadable after {STATE_NAG_SEC/60:.0f} min "
                            f"- this campaign's subscriber data will need "
                            f"recover_subscriber.py to be usable")
            # Rotation must never end the process. A momentary failure to
            # open the new file - the output share blinking, a stray
            # character in a run id - used to escape to the outer handler and
            # exit with status 0, which looks like a clean stop to anything
            # supervising it. Keep writing to the file we already have and
            # retry on the next sample.
            try:
                writer.switch(safe_name(run_id))
            except Exception as exc:
                if run_id != last_switch_fail:
                    last_switch_fail = run_id
                    events.write("ROTATE_FAILED",
                                 f"could not open the file for run {run_id}: "
                                 f"{type(exc).__name__}: {str(exc)[:120]} - "
                                 f"still writing to "
                                 f"{writer.run_id or 'no file'}; will retry")
                run_id = writer.run_id or run_id

            ph = {"level_id": args.level_id, "phase_state": "adhoc", "clients": 0,
                  "rows_per_commit": 0, "row_bytes": 0, "target_wal_mbps": 0,
                  "network_latency_ms": 0}
            if not args.no_phase_file:
                got = read_json(args.phase_file)
                if got:
                    ph.update(got)
            if ph.get("level_id") != last_level:
                events.write("LEVEL", f"{last_level} -> {ph.get('level_id')} "
                                      f"(state={ph.get('phase_state')})", echo=False)
                last_level = ph.get("level_id")

            row = {"sample_time": datetime.now(timezone.utc).isoformat(),
                   "epoch": t0, "run_id": run_id, "host": host}
            row.update({k: ph.get(k) for k in COMMON[4:]})

            # ---- sample -----------------------------------------------------
            ok = False
            try:
                if args.role == "publisher":
                    m = None
                    for _attempt in range(3):
                        if not db.ensure():
                            break
                        m = db.query(db.pub_sql, tag="pub_sample")
                        if m is not None:
                            break
                        # The sample query is the one that matters. If it was
                        # rejected because a column does not exist on this
                        # server, step the WAL-timing clause down and retry
                        # immediately rather than losing the sample.
                        # Only a genuine rejection may retire a timing source.
                        # Treating a dropped connection as "unsupported" used
                        # to blank these columns permanently after one blip.
                        if db.last_error_kind != "unsupported":
                            break
                        if not db.degrade_timing("column not present on this server"):
                            break
                    if m is not None:
                        row.update(m)
                        ok = True
                        r = db.query(PUB_REPL_SQL, tag="pub_repl")
                        if r:
                            row.update({
                                "repl_state": r.get("state"), "sent_lsn": r.get("sent_lsn"),
                                "write_lsn": r.get("write_lsn"), "replay_lsn": r.get("replay_lsn"),
                                "send_lag_bytes": r.get("send_lag_bytes"),
                                "write_lag_bytes": r.get("write_lag_bytes"),
                                "flush_lag_bytes": r.get("flush_lag_bytes"),
                                "replay_lag_bytes": r.get("replay_lag_bytes"),
                                "write_lag_sec": r.get("write_lag_sec"),
                                "flush_lag_sec": r.get("flush_lag_sec"),
                                "replay_lag_sec": r.get("replay_lag_sec")})
                            if r.get("state") != last_state:
                                events.write("REPL_STATE", f"{last_state} -> {r.get('state')}")
                                last_state = r.get("state")
                        elif r == {} and last_state != "DOWN":
                            events.write("REPLICATION_DOWN",
                                         "no active row in pg_stat_replication")
                            last_state = "DOWN"

                        s = db.query(PUB_SLOT_SQL, tag="pub_slot")
                        if s:
                            row.update({"slot_active": s.get("active"),
                                        "retained_wal_bytes": s.get("retained_wal_bytes"),
                                        "wal_status": s.get("wal_status")})
                            if s.get("wal_status") != last_status:
                                events.write("SLOT_STATUS",
                                             f"{last_status} -> {s.get('wal_status')} "
                                             f"(retained {(s.get('retained_wal_bytes') or 0)/1e6:.1f} MB)")
                                last_status = s.get("wal_status")
                            if s.get("wal_status") in ("lost", "unreserved"):
                                events.write("SLOT_INVALID",
                                             f"wal_status={s.get('wal_status')}")
                else:
                    m = db.query(SUB_SQL, tag="sub_sample")
                    if m is not None:
                        row.update(m)
                        ok = True
                        if args.row_count_interval > 0 and t0 >= next_row_count:
                            next_row_count = t0 + args.row_count_interval
                            rc = db.query_scalar_bounded(
                                SUB_ROWCOUNT_SQL, "sub_row_count",
                                ROW_COUNT_TIMEOUT_MS)
                            if rc is not None:
                                row["row_count"] = rc
                                if last_rc is not None and rc >= last_rc[1]:
                                    row["row_count_delta"] = rc - last_rc[1]
                                last_rc = (t0, rc)
                            else:
                                # Too slow or unavailable. Try again on the
                                # next tick rather than on the next interval:
                                # a table that big is exactly when the count
                                # is most interesting.
                                next_row_count = t0 + args.interval
                        r = db.query(SUB_STAT_SQL, tag="sub_stat")
                        if r:
                            row.update(r)
            except Exception:
                events.write("SAMPLE_EXCEPTION", traceback.format_exc(limit=2).replace("\n", " | "))

            row["db_connected"] = int(bool(ok))

            # ---- derived rates ---------------------------------------------
            # Guarded: this arithmetic runs on values straight from the
            # database. One unexpected type must never kill a monitor that
            # is meant to run unattended for a day.
            try:
                if prev is not None and ok:
                    dt = t0 - prev["epoch"]
                    row["interval_sec"] = dt
                    if dt > GAP_WARN_SEC:
                        events.write("SAMPLE_GAP", f"{dt:.1f}s between samples", echo=False)
                    if dt > 0:
                        # A server crash-and-recover resets the cumulative
                        # counters. Differencing across that point used to
                        # write -619 MB/s and -38,000 commits/s into the
                        # scientific output with nothing logged.
                        reset_key = ("wal_stats_reset" if args.role == "publisher"
                                     else "db_stats_reset")
                        counters_reset = False
                        a_r, b_r = row.get(reset_key), prev.get(reset_key)
                        if a_r is not None and b_r is not None and a_r != b_r:
                            counters_reset = True
                        for _k in ("wal_bytes", "xact_commit", "tup_inserted"):
                            a_v, b_v = row.get(_k), prev.get(_k)
                            if a_v is not None and b_v is not None and a_v < b_v:
                                counters_reset = True
                        if counters_reset:
                            events.write("COUNTER_RESET",
                                         "server statistics were reset between "
                                         "samples; derived rates for this sample "
                                         "are left empty")

                        def delta(k):
                            a, b = row.get(k), prev.get(k)
                            if a is None or b is None or counters_reset:
                                return None
                            return a - b

                        def rate(k):
                            d = delta(k)
                            return (d / dt) if d is not None else None
                        if args.role == "publisher":
                            wd = delta("wal_bytes")
                            row["wal_bytes_delta"] = wd
                            row["wal_bytes_per_sec"] = (wd / dt) if wd is not None else None
                            row["wal_mb_per_sec"] = (wd / dt / 1_048_576) if wd is not None else None
                            # These are DELTAS. They used to hold per-second
                            # rates identical to the *_per_sec columns beside
                            # them, and rows_inserted_delta was never assigned
                            # at all.
                            row["commits_delta"] = delta("xact_commit")
                            row["rows_inserted_delta"] = delta("tup_inserted")
                            row["commits_per_sec"] = rate("xact_commit")
                            row["rows_inserted_per_sec"] = rate("tup_inserted")
                            row["rows_updated_per_sec"] = rate("tup_updated")
                            row["rows_deleted_per_sec"] = rate("tup_deleted")
                            row["wal_sync_time_delta_ms"] = delta("wal_sync_time_ms")
                            a, b = lsn_to_int(row.get("current_wal_lsn")), lsn_to_int(prev.get("current_wal_lsn"))
                            row["lsn_bytes_per_sec"] = ((a - b) / dt) if (a and b) else None
                            if (row.get("replay_lag_bytes") is not None
                                    and prev.get("replay_lag_bytes") is not None):
                                row["d_replay_lag_bytes_per_sec"] = (
                                    row["replay_lag_bytes"] - prev["replay_lag_bytes"]) / dt
                        else:
                            row["commits_per_sec"] = rate("xact_commit")
                            row["rows_applied_per_sec"] = rate("tup_inserted")
                            # row_count_delta is set where the count is taken;
                            # delta() here would compare against a sample that
                            # carries no count and always yield None.

            except Exception:
                events.write("DERIVED_RATE_ERROR",
                             traceback.format_exc(limit=2).replace("\n", " | "))
            hr, prev_host = host_rates(prev_host, t0, args.data_volume)
            row.update(hr)
            fg = hr.get("data_volume_free_gb")
            if fg is not None and fg < args.disk_low_gb:
                events.write("DISK_LOW",
                             f"{fg} GB free on {args.data_volume} "
                             f"(floor {args.disk_low_gb:g} GB)")

            writer.row(row)
            n_total += 1
            if ok:
                prev = row

            sl = args.interval - (time.time() - t0)
            if sl > 0:
                time.sleep(sl)

    except KeyboardInterrupt:
        events.write("STOP", "interrupted by user")
    except Exception:
        fatal = True
        events.write("FATAL", traceback.format_exc().replace("\n", " | "))
    finally:
        el = time.time() - t_begin
        # total_counts, not counts: counts is reset on every file rotation,
        # so a --follow run that spanned sixty levels used to summarise the
        # last one only.
        events.write("SUMMARY", f"samples={n_total} elapsed={el:.0f}s "
                                f"events={json.dumps(events.total_counts)}")
        writer.close()
        events.close()
        db.close()
        print(f"\n[monitor] {n_total} samples over {el/3600:.2f} h")

    # A non-zero code so a supervisor can tell an unplanned death from a
    # requested stop. Returning 0 for both meant nothing ever restarted it.
    return 9 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())
