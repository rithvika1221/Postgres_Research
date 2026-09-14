#!/usr/bin/env python3
"""
Round 3 - the ONE authoritative table of expected PostgreSQL settings.

This file is the machine-readable form of postgresql_settings.md. Nothing else
in the harness should carry a hard-coded expected setting value: check_config.py
and run_region.py both import this, so "what the testbed is supposed to be" has
exactly one definition and cannot drift between the document and the checks.

WHY THIS EXISTS
---------------
The Central US control subscriber was found to be running stock PostgreSQL
defaults - shared_buffers 128MB against the specified 8GB, checkpoint_timeout
5min against 15min, track_io_timing off - while eastus, built by
bootstrap_subscriber.ps1, had the full specified block. Nothing reported it,
because nothing was comparing the running server against the specification.
Two subscribers differing by a factor of sixty-four in shared_buffers is not a
latency study; it is a memory study with a latency label.

SEVERITY
--------
  physics   changes what is being measured. Apply cost, checkpoint behaviour,
            WAL volume, worker counts. A mismatch here invalidates the
            comparison between regions and must block a run.
  monitor   does not change the physics but blanks or zeroes a column the
            monitor records, so that column exists for three regions and not
            the fourth. Blocks, because a missing column is discovered during
            analysis, which is the worst time.
  recorded  reported in Methods and must be identical across machines, but
            does not by itself move the measurement. Warns.

Each entry is (name, expected, severity, note).
"""

# --- publisher ---------------------------------------------------------------
PUBLISHER = [
    ("wal_level", "logical", "physics",
     "logical replication cannot work without it"),
    ("wal_compression", "off", "physics",
     "on makes payload size meaningless - row_bytes stops being the "
     "independent variable it claims to be"),
    ("synchronous_commit", "off", "physics",
     "the subject is apply lag, not commit durability. Declared in Methods"),
    ("max_wal_size", "16GB", "physics",
     "smaller values cause checkpoint storms inside a 360 s level, which "
     "inflate WAL with full-page images and make the closed loop throttle "
     "the workload below its set point"),
    ("min_wal_size", "2GB", "physics", ""),
    ("checkpoint_timeout", "15min", "physics",
     "the default 5min puts two to three checkpoints inside every level"),
    ("checkpoint_completion_target", "0.9", "physics", ""),
    ("shared_buffers", "8GB", "physics",
     "25 percent of 32 GiB. The default 128MB changes read and write "
     "amplification by more than the independent variable does"),
    ("effective_cache_size", "24GB", "physics", ""),
    ("work_mem", "64MB", "physics", ""),
    ("maintenance_work_mem", "2GB", "physics", ""),
    ("max_wal_senders", "10", "recorded", ""),
    ("max_replication_slots", "10", "recorded", ""),
    ("wal_keep_size", "8GB", "recorded",
     "slot retention headroom for above-knee runs"),
    ("wal_sender_timeout", "60s", "recorded",
     "at 200 ms RTT a shorter timeout can drop a healthy walsender"),
    ("max_connections", "200", "recorded", ""),
    ("track_counts", "on", "monitor",
     "without it pg_stat_database is frozen and the monitor's commit and "
     "tuple rates are all zero"),
    ("track_io_timing", "on", "monitor",
     "blk_read_time and blk_write_time are blank without it"),
    ("track_wal_io_timing", "on", "monitor",
     "WAL write and fsync timing. On PostgreSQL 18 this feeds pg_stat_io "
     "with object='wal', not pg_stat_wal"),
    ("log_checkpoints", "on", "monitor",
     "a checkpoint inside a level must be visible in the log, or a set-point "
     "miss cannot be explained afterwards"),
    ("logging_collector", "on", "recorded", ""),
    ("log_min_duration_statement", "-1", "recorded",
     "logging every statement at 4000 commits per second would itself "
     "become the bottleneck"),
]

# --- subscriber --------------------------------------------------------------
SUBSCRIBER = [
    ("wal_level", "logical", "recorded",
     "the subscriber never publishes, so replica would work. It is specified "
     "as logical so all four subscribers are byte-identical in configuration "
     "and the Methods sentence is true as written"),
    ("synchronous_commit", "off", "recorded",
     "the subscription's own synchronous_commit=off overrides this GUC for "
     "the apply worker, so a mismatch here does not corrupt the apply path - "
     "but it is still a difference between machines that the paper claims "
     "are identically configured"),
    ("max_replication_slots", "10", "recorded", ""),
    ("max_logical_replication_workers", "4", "physics",
     "how many apply workers may exist at all"),
    ("max_worker_processes", "16", "physics",
     "caps the above; too low and an apply worker silently fails to start"),
    ("max_sync_workers_per_subscription", "2", "physics", ""),
    ("max_parallel_apply_workers_per_subscription", "2", "physics",
     "with streaming=off no parallel apply worker is used, but the value is "
     "part of the recorded configuration and must match"),
    ("max_wal_size", "16GB", "physics",
     "the subscriber writes WAL for everything it applies"),
    ("min_wal_size", "2GB", "physics", ""),
    ("checkpoint_timeout", "15min", "physics",
     "a checkpoint on the subscriber stalls apply, which appears as a lag "
     "spike indistinguishable from a network event"),
    ("shared_buffers", "8GB", "physics",
     "apply reads and updates three secondary indexes per row. At the "
     "default 128MB almost every index page touch is a disk read"),
    ("effective_cache_size", "24GB", "physics", ""),
    ("work_mem", "64MB", "physics", ""),
    ("maintenance_work_mem", "2GB", "physics", ""),
    ("max_connections", "200", "recorded", ""),
    ("track_counts", "on", "monitor",
     "without it the subscriber monitor's rows_applied_per_sec is zero for "
     "the whole run"),
    ("track_io_timing", "on", "monitor", ""),
    ("track_wal_io_timing", "on", "monitor", ""),
    ("log_checkpoints", "on", "monitor", ""),
    ("logging_collector", "on", "recorded", ""),
]

SPEC = {"publisher": PUBLISHER, "subscriber": SUBSCRIBER}
BLOCKING = ("physics", "monitor")

# pg_settings.unit -> multiplier into a canonical base (bytes, or ms)
_MEM = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4,
        "8kB": 8 * 1024, "16kB": 16 * 1024, "32kB": 32 * 1024,
        "64kB": 64 * 1024, "8MB": 8 * 1024 ** 2, "16MB": 16 * 1024 ** 2,
        "32MB": 32 * 1024 ** 2, "64MB": 64 * 1024 ** 2}
_TIME = {"ms": 1, "s": 1000, "min": 60000, "h": 3600000, "d": 86400000}


def _split(text):
    """'8GB' -> (8.0, 'GB');  '0.9' -> (0.9, '')"""
    t = str(text).strip()
    i = 0
    while i < len(t) and (t[i].isdigit() or t[i] in "-+."):
        i += 1
    num = t[:i] or "0"
    try:
        return float(num), t[i:].strip()
    except ValueError:
        return None, t


def normalise(expected, actual_setting, unit):
    """Put an expected string and a pg_settings value on the same scale.

    pg_settings reports a number in its OWN unit ('8kB' for shared_buffers,
    'min' for checkpoint_timeout), while the specification is written the way
    a human writes it ('8GB', '15min'). Comparing the strings says 8GB does
    not equal 1048576 and is useless. Returns (expected_value, actual_value)
    as comparable numbers, or (expected_text, actual_text) for booleans and
    plain strings.
    """
    unit = (unit or "").strip()
    if not unit:
        e_num, e_unit = _split(expected)
        if e_num is not None and not e_unit:
            try:
                return e_num, float(actual_setting)
            except (TypeError, ValueError):
                pass
        return str(expected).strip().lower(), str(actual_setting).strip().lower()

    scale = _MEM.get(unit) or _TIME.get(unit)
    if scale is None:
        return str(expected).strip().lower(), str(actual_setting).strip().lower()

    e_num, e_unit = _split(expected)
    if e_num is None:
        return str(expected).strip().lower(), str(actual_setting).strip().lower()
    e_scale = _MEM.get(e_unit) or _TIME.get(e_unit)
    if e_scale is None:
        # A bare number against a unit-bearing setting means "this many of the
        # server's own units", which is how postgresql.conf reads it too.
        e_scale = scale
    try:
        return e_num * e_scale, float(actual_setting) * scale
    except (TypeError, ValueError):
        return str(expected), str(actual_setting)


def pretty(setting, unit):
    """Render a pg_settings value the way a human wrote it in the conf file."""
    unit = (unit or "").strip()
    if not unit:
        return str(setting)
    scale = _MEM.get(unit)
    if scale:
        b = float(setting) * scale
        for name, div in (("TB", 1024 ** 4), ("GB", 1024 ** 3),
                          ("MB", 1024 ** 2), ("kB", 1024)):
            if b >= div and b % div == 0:
                return f"{int(b // div)}{name}"
        return f"{int(b)}B"
    scale = _TIME.get(unit)
    if scale:
        ms = float(setting) * scale
        for name, div in (("h", 3600000), ("min", 60000), ("s", 1000)):
            if ms >= div and ms % div == 0:
                return f"{int(ms // div)}{name}"
        return f"{int(ms)}ms"
    return f"{setting}{unit}"


# ---------------------------------------------------------------------------
# Overrides
# ---------------------------------------------------------------------------
# The table above describes the REAL testbed: 8-vCPU, 32 GiB Azure VMs. A
# smaller machine - the Linux test rig, or any future replication of this work
# on different hardware - cannot satisfy shared_buffers = 8GB, and pretending
# otherwise would mean every check fails for a reason that has nothing to do
# with the experiment.
#
# So an override file may PATCH individual values. It can only change the
# expected value, never remove a setting and never lower its severity, and
# whenever one is in use every tool that reads this table announces it loudly
# and names the file. An override is a recorded decision about different
# hardware, not a way to make a red check turn green on the real testbed.
#
#   R3_PG_SPEC=/path/to/spec.json
#
#   {"publisher":  {"shared_buffers": "512MB", "work_mem": "4MB"},
#    "subscriber": {"shared_buffers": "512MB"}}

import json as _json
import os as _os


def load():
    """Return (spec_dict, source_description, overridden_names)."""
    path = _os.environ.get("R3_PG_SPEC")
    base = {r: [tuple(x) for x in rows] for r, rows in SPEC.items()}
    if not path:
        return base, "built-in (the real 32 GiB testbed)", {}
    try:
        with open(path, encoding="utf-8") as fh:
            patch = _json.load(fh)
    except Exception as exc:
        raise SystemExit(f"R3_PG_SPEC={path} could not be read: {exc}")

    applied = {}
    for role in ("publisher", "subscriber"):
        want = patch.get(role) or {}
        if not isinstance(want, dict):
            raise SystemExit(f"R3_PG_SPEC: '{role}' must be an object of "
                             f"setting -> expected value")
        known = {n for n, *_ in base[role]}
        unknown = sorted(set(want) - known)
        if unknown:
            raise SystemExit(f"R3_PG_SPEC: '{role}' names settings that are "
                             f"not in the specification and so cannot be "
                             f"overridden: {', '.join(unknown)}")
        rows = []
        for name, expected, severity, note in base[role]:
            if name in want:
                rows.append((name, str(want[name]), severity, note))
                applied.setdefault(role, {})[name] = (expected,
                                                      str(want[name]))
            else:
                rows.append((name, expected, severity, note))
        base[role] = rows
    return base, path, applied


def announce(source, applied, printer=print):
    """Say, unmissably, that the expected values are not the study's own."""
    if not applied:
        return
    printer("")
    printer("  !!  SPECIFICATION OVERRIDE IN USE")
    printer(f"  !!  {source}")
    for role in sorted(applied):
        for name in sorted(applied[role]):
            was, now = applied[role][name]
            printer(f"  !!    {role}: {name}  {was} -> {now}")
    printer("  !!  These are NOT the values postgresql_settings.md specifies.")
    printer("  !!  Correct for different hardware; wrong for the real testbed.")
    printer("")
