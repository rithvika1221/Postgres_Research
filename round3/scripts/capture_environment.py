#!/usr/bin/env python3
"""
Round 3 - environment capture.  STANDALONE: imports nothing from sibling scripts.

Run this ONCE per host before the first experiment, and again after any change
to hardware or configuration. It writes a complete machine-readable record of
the testbed.

Every Methods detail the reviewer said was missing is captured here: exact
PostgreSQL minor version, every non-default setting, subscription parameters
including parallel-apply status, OS build, CPU, RAM, disk layout and free
space, clock offset, Python and driver versions.

Usage:
    set R3_PUB_DSN=host=localhost dbname=pub user=postgres password=...
    py capture_environment.py --role publisher

    py capture_environment.py --role subscriber --dsn "host=... dbname=sub ..."
"""

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

try:
    import psutil
except ImportError:
    psutil = None

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")

# Settings the manuscript must report. Anything the reviewer could ask about.
SETTINGS_OF_RECORD = [
    "server_version", "wal_level", "synchronous_commit", "wal_compression",
    "max_wal_size", "min_wal_size", "checkpoint_timeout",
    "checkpoint_completion_target", "wal_keep_size", "wal_buffers",
    "max_wal_senders", "max_replication_slots", "wal_sender_timeout",
    "shared_buffers", "effective_cache_size", "work_mem", "maintenance_work_mem",
    "max_connections", "max_worker_processes",
    "max_logical_replication_workers", "max_sync_workers_per_subscription",
    "max_parallel_apply_workers_per_subscription",
    "track_counts", "track_io_timing", "track_wal_io_timing",
    "autovacuum", "autovacuum_naptime", "full_page_writes",
    "fsync", "commit_delay", "commit_siblings", "listen_addresses",
]


def sh(cmd):
    """Run a shell command, return stripped output or an error marker."""
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT,
                                       text=True, timeout=30).strip()
    except Exception as exc:
        return f"<unavailable: {type(exc).__name__}>"


def collect_host():
    info = {
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "psycopg2_version": psycopg2.__version__,
    }
    if psutil:
        info.update({
            "cpu_physical_cores": psutil.cpu_count(logical=False),
            "cpu_logical_cores": psutil.cpu_count(logical=True),
            "total_memory_bytes": psutil.virtual_memory().total,
            "total_memory_gb": round(psutil.virtual_memory().total / 1024**3, 1),
        })
        try:
            f = psutil.cpu_freq()
            if f:
                info["cpu_max_mhz"] = f.max
        except Exception:
            pass
        parts = []
        for p in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(p.mountpoint)
                parts.append({
                    "device": p.device, "mountpoint": p.mountpoint,
                    "fstype": p.fstype, "opts": p.opts,
                    "total_gb": round(u.total / 1024**3, 1),
                    "free_gb": round(u.free / 1024**3, 1),
                    "percent_used": u.percent,
                })
            except Exception:
                pass
        info["disk_partitions"] = parts
    else:
        info["psutil"] = "NOT INSTALLED - install with: py -m pip install psutil"

    if platform.system() == "Windows":
        info["windows"] = {
            # Clock offset. Round 2 produced negative lag-time samples from skew
            # between the two hosts; this is the number that proves it is fixed.
            "w32tm_status": sh("w32tm /query /status"),
            "os_caption": sh('powershell -NoProfile -Command "(Get-CimInstance Win32_OperatingSystem).Caption"'),
            "os_build": sh('powershell -NoProfile -Command "(Get-CimInstance Win32_OperatingSystem).BuildNumber"'),
            "physical_disks": sh('powershell -NoProfile -Command "Get-PhysicalDisk | Select-Object FriendlyName,MediaType,Size | ConvertTo-Json -Compress"'),
            "volumes": sh('powershell -NoProfile -Command "Get-Volume | Select-Object DriveLetter,FileSystemLabel,Size,SizeRemaining | ConvertTo-Json -Compress"'),
        }
    else:
        info["linux"] = {"uname": sh("uname -a"), "lsblk": sh("lsblk -J 2>/dev/null")}
    return info


def collect_db(dsn, role):
    out = {"role": role}
    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT version() AS v, current_database() AS db, "
                "current_setting('server_version') AS sv, "
                "current_setting('server_version_num') AS svn")
    r = cur.fetchone()
    out["version_string"] = r["v"]
    out["database"] = r["db"]
    out["server_version"] = r["sv"]
    out["server_version_num"] = r["svn"]

    major = int(r["svn"]) // 10000
    out["major_version"] = major
    if major != 18:
        out["VERSION_WARNING"] = (
            f"Expected PostgreSQL 18, found {major}. Round 3 is single-version "
            f"by design - do not mix.")

    cur.execute("""
        SELECT name, setting, unit, source, boot_val, reset_val
        FROM pg_settings WHERE name = ANY(%s) ORDER BY name
    """, (SETTINGS_OF_RECORD,))
    out["settings"] = {row["name"]: {
        "setting": row["setting"], "unit": row["unit"],
        "source": row["source"], "default": row["boot_val"],
        "is_default": row["setting"] == row["boot_val"],
    } for row in cur.fetchall()}

    # everything explicitly changed from the shipped default
    cur.execute("""
        SELECT name, setting, unit, source FROM pg_settings
        WHERE source NOT IN ('default','override') AND name NOT LIKE 'lc_%'
        ORDER BY name
    """)
    out["non_default_settings"] = [dict(x) for x in cur.fetchall()]

    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS s")
    out["database_size"] = cur.fetchone()["s"]

    try:
        cur.execute("SHOW data_directory")
        out["data_directory"] = cur.fetchone()["data_directory"]
    except Exception:
        out["data_directory"] = "<requires superuser>"

    if role == "publisher":
        cur.execute("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, "
                    "pubtruncate FROM pg_publication")
        out["publications"] = [dict(x) for x in cur.fetchall()]
        cur.execute("SELECT schemaname, tablename FROM pg_publication_tables")
        out["published_tables"] = [dict(x) for x in cur.fetchall()]
        cur.execute("""SELECT slot_name, plugin, slot_type, active, wal_status,
                       temporary FROM pg_replication_slots""")
        out["replication_slots"] = [dict(x) for x in cur.fetchall()]
        cur.execute("""SELECT application_name, state, sync_state, backend_start
                       FROM pg_stat_replication""")
        out["replication_connections"] = [
            {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(x).items()}
            for x in cur.fetchall()]
    else:
        cur.execute("""SELECT subname, subenabled, subbinary, substream,
                       subslotname, subsynccommit, subpublications
                       FROM pg_subscription""")
        out["subscriptions"] = [dict(x) for x in cur.fetchall()]
        try:
            cur.execute("SELECT subname, received_lsn::text, latest_end_lsn::text "
                        "FROM pg_stat_subscription")
            out["subscription_status"] = [dict(x) for x in cur.fetchall()]
        except Exception:
            pass

    # table + index inventory, so the schema is documented, not described
    try:
        cur.execute("""
            SELECT c.relname AS table_name, a.attname AS column_name,
                   format_type(a.atttypid, a.atttypmod) AS data_type,
                   a.attnotnull AS not_null
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = 'public' AND c.relkind = 'r' AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
        """)
        out["schema_columns"] = [dict(x) for x in cur.fetchall()]
        cur.execute("SELECT tablename, indexname, indexdef FROM pg_indexes "
                    "WHERE schemaname = 'public' ORDER BY tablename, indexname")
        out["schema_indexes"] = [dict(x) for x in cur.fetchall()]
        cur.execute("""SELECT c.relname, c.relreplident FROM pg_class c
                       JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname='public' AND c.relkind='r'""")
        out["replica_identity"] = [dict(x) for x in cur.fetchall()]
    except Exception as exc:
        out["schema_error"] = str(exc)

    cur.close()
    conn.close()
    return out


def main():
    ap = argparse.ArgumentParser(description="Capture the Round 3 testbed environment.")
    ap.add_argument("--role", required=True, choices=["publisher", "subscriber"])
    ap.add_argument("--dsn", default=None,
                    help="defaults to $R3_PUB_DSN or $R3_SUB_DSN for the role")
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--tag", default="", help="optional label, e.g. 'post-calibration'")
    args = ap.parse_args()

    dsn = args.dsn or os.environ.get(
        "R3_PUB_DSN" if args.role == "publisher" else "R3_SUB_DSN")
    if not dsn:
        env = "R3_PUB_DSN" if args.role == "publisher" else "R3_SUB_DSN"
        print(f"ERROR: no DSN. Pass --dsn or set {env}.")
        return 2

    # ---- is the host section actually about the machine in the DSN? --------
    # collect_host() reads THIS machine: socket.gethostname(), platform and
    # psutil. The PostgreSQL section reads whatever the DSN points at. When
    # those are two different machines - which is exactly what
    # start_campaign.ps1 does when it runs "--role subscriber" on the
    # publisher - the file records the PUBLISHER's CPU, RAM, disks and
    # hostname under the label "subscriber".
    #
    # That does not matter much when both machines sit in one rack and one
    # subnet. It matters a great deal for family F, whose entire Methods claim
    # is that the subscriber hardware is identical in every region: the file
    # meant to evidence that would contain four copies of the publisher's
    # hardware instead.
    #
    # So say so, in the file and on the console, rather than silently
    # producing a plausible wrong answer.
    dsn_host = ""
    for tok in dsn.split():
        if tok.startswith("host="):
            dsn_host = tok[5:]
            break
    local_names = {"", "localhost", "127.0.0.1", "::1", "/tmp",
                   socket.gethostname().lower(), socket.getfqdn().lower()}
    host_is_local = dsn_host.lower() in local_names
    if not host_is_local:
        try:
            for fam, _, _, _, sa in socket.getaddrinfo(socket.gethostname(), None):
                if sa[0] == dsn_host:
                    host_is_local = True
                    break
        except Exception:
            pass

    os.makedirs(args.out, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_{args.tag}" if args.tag else ""
    path = os.path.join(args.out, f"environment_{args.role}{suffix}_{stamp}.json")

    print(f"[env] capturing {args.role} environment ...")
    doc = {
        "captured_utc": datetime.now(timezone.utc).isoformat(),
        "role": args.role,
        "tag": args.tag,
        "round": 3,
        "postgres_major_expected": 18,
        "host": collect_host(),
        "postgres": collect_db(dsn, args.role),
        "host_section_provenance": {
            "dsn_host": dsn_host,
            "describes_the_dsn_machine": host_is_local,
            "note": ("The 'host' section always describes the machine this "
                     "script ran on. When describes_the_dsn_machine is false, "
                     "it is NOT the hardware of the database in 'postgres' - "
                     "re-run this script ON that machine to get its hardware."),
        },
    }

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, default=str)

    pg = doc["postgres"]
    h = doc["host"]
    if not host_is_local:
        print()
        print(f"[env] !! WARNING: the DSN points at '{dsn_host}', but the host")
        print(f"[env] !! section below describes {h['hostname']}, the machine this")
        print(f"[env] !! script ran on. The CPU, RAM and disk figures in this file")
        print(f"[env] !! are NOT the {args.role}'s.")
        print(f"[env] !! Re-run  py capture_environment.py --role {args.role}  ON that")
        print(f"[env] !! machine if you need its hardware recorded. This matters for")
        print(f"[env] !! family F, where identical subscriber hardware is the claim.")
        print()
    print(f"[env] host      : {h['hostname']}  {h.get('platform')}")
    print(f"[env] cpu/ram   : {h.get('cpu_logical_cores')} vCPU / {h.get('total_memory_gb')} GB")
    print(f"[env] postgres  : {pg['server_version']} (num {pg['server_version_num']})")
    print(f"[env] datadir   : {pg.get('data_directory')}")
    if "VERSION_WARNING" in pg:
        print(f"[env] !! {pg['VERSION_WARNING']}")
    if psutil is None:
        print("[env] !! psutil missing - host hardware detail is incomplete")
    print(f"[env] written   : {path}")

    # free-space sanity, since a full data volume ruins a long run
    if psutil:
        for p in h.get("disk_partitions", []):
            if p["free_gb"] < 50:
                print(f"[env] !! low free space on {p['mountpoint']}: {p['free_gb']} GB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
