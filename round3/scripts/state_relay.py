#!/usr/bin/env python3
"""
Copy the publisher's campaign state to this subscriber, over the PostgreSQL
connection.  STANDALONE.  Read-only on the publisher.  Run on the SUBSCRIBER,
in its own window, alongside the monitor.

Why this exists
---------------
monitor.py --follow reads C:\\r3\\campaign_state.json to learn which run and
level it is sampling. On the local subscriber that file is fetched over SMB
from \\\\R3PUB\\r3. A subscriber in another Azure region cannot do that:
opening 445 across a VNet peering means punching a second hole in the NSG and
the Windows firewall for a protocol that needs five or six round trips per
read - at 200 ms each, one state read would stall a 1 Hz sampler for over a
second, every second.

Without the state file the monitor still records everything, but every row is
labelled 'pending' with no run_id and no level_id, and the data has to be
rescued afterwards by joining on timestamps. That rescue was done once on this
project already, on family E, and recovered 23-27% of the samples.

So instead this pulls the file through the one port that is already open and
already proven - 5432 - using pg_read_file() on the publisher, and writes it
where the monitor already looks. One small query every few seconds. The
monitor needs no changes and runs with its default local --state-file.

Requires the publisher role to be superuser or a member of
pg_read_server_files. postgres is.

  py state_relay.py                       # uses $R3_PUB_DSN
  py state_relay.py --interval 2
  py state_relay.py --root C:\\r3

Stop it with:  New-Item C:\\r3\\STOP -ItemType File   (the same stop file the
monitor uses, so one command stops both), or Ctrl-C.

Exit codes
----------
  0  stopped cleanly
  1  interrupted before it ever succeeded
  2  cannot start - bad DSN, or no permission to read files on the publisher
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import psycopg2

TOOL_VERSION = "v1 2026-09-08"

DEFAULT_PUB = os.environ.get("R3_PUB_DSN", "")

# The two files the monitor follows. phase_state.json is optional - it only
# carries the load/drain phase label - but it costs nothing to bring across
# and validate_run.py uses the phase to exclude the ramp.
FILES = ["campaign_state.json", "phase_state.json"]

# pg_read_file wants forward slashes even on Windows.
PUB_ROOT = "C:/r3"

# Don't rewrite an unchanged file: the monitor stats it every second and a
# needless write invites a torn read at exactly the wrong moment.
MAX_BYTES = 1_000_000

NAG_SEC = 300.0


def read_remote(cur, pub_root, name):
    path = pub_root.rstrip("/\\").replace("\\", "/") + "/" + name
    cur.execute("SELECT pg_read_file(%s, 0, %s)", (path, MAX_BYTES))
    row = cur.fetchone()
    return row[0] if row else None


def write_local(path, text):
    """Write via a temporary file and replace, so the monitor never sees a
    half-written state file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser(
        description="Relay the publisher's campaign state to this subscriber "
                    "over port 5432 (run on the subscriber).")
    ap.add_argument("--dsn", default=DEFAULT_PUB,
                    help="DSN of the PUBLISHER from this machine. Defaults to "
                         "$R3_PUB_DSN.")
    ap.add_argument("--root", default=r"C:\r3",
                    help="where to write the files on THIS machine. Must be the "
                         "directory monitor.py reads, i.e. its --state-file's "
                         "directory. Default C:\\r3")
    ap.add_argument("--pub-root", default=PUB_ROOT,
                    help="where C:\\r3 lives on the PUBLISHER, forward slashes.")
    ap.add_argument("--interval", type=float, default=3.0,
                    help="seconds between pulls. The monitor rotates files on a "
                         "run change, so a few seconds of lag costs at most a "
                         "few mislabelled samples at a run boundary, which "
                         "validate_run.py already excludes as ramp.")
    ap.add_argument("--stop-file", default=None,
                    help="default <root>\\STOP - the same file that stops the "
                         "monitor.")
    ap.add_argument("--once", action="store_true",
                    help="pull once and exit. Use this to test permissions "
                         "before starting a campaign.")
    args = ap.parse_args()

    stop_file = args.stop_file or os.path.join(args.root, "STOP")

    print(f"[state_relay] {TOOL_VERSION}")
    if not args.dsn:
        print("FAIL: no DSN. Set R3_PUB_DSN to the PUBLISHER, or pass --dsn.")
        return 2

    try:
        os.makedirs(args.root, exist_ok=True)
    except Exception as exc:
        print(f"FAIL: cannot create {args.root} - {exc}")
        return 2

    host = args.dsn.split("host=")[-1].split()[0] if "host=" in args.dsn else "?"
    print(f"[state_relay] publisher {host}  ->  {args.root}")
    print(f"[state_relay] every {args.interval:.0f}s; stop with: "
          f"New-Item {stop_file} -ItemType File")

    conn = None
    last_seen = {}
    ever_ok = False
    last_nag = 0.0
    last_run = None

    try:
        while True:
            if os.path.exists(stop_file):
                print("[state_relay] stop file present - exiting")
                break

            try:
                if conn is None or conn.closed:
                    conn = psycopg2.connect(args.dsn, connect_timeout=15,
                                            application_name="r3_state_relay")
                    conn.set_session(autocommit=True, readonly=True)
                cur = conn.cursor()
                found = []
                for name in FILES:
                    try:
                        text = read_remote(cur, args.pub_root, name)
                    except psycopg2.Error as exc:
                        # A missing phase_state.json is normal between runs.
                        if "No such file" in str(exc) or "does not exist" in str(exc):
                            conn.rollback()
                            continue
                        raise
                    if text is None:
                        continue
                    found.append(name)
                    if last_seen.get(name) == text:
                        continue
                    write_local(os.path.join(args.root, name), text)
                    last_seen[name] = text
                    if name == "campaign_state.json":
                        try:
                            rid = json.loads(text).get("current_run_id")
                        except Exception:
                            rid = None
                        if rid and rid != last_run:
                            last_run = rid
                            print(f"[state_relay] "
                                  f"{datetime.now().strftime('%H:%M:%S')}  "
                                  f"run -> {rid}")
                cur.close()
                ever_ok = True
                if args.once:
                    if found:
                        print(f"[state_relay] pulled: {', '.join(found)} -> {args.root}")
                        print("[state_relay] permissions and path are good.")
                        return 0
                    print(f"[state_relay] connected and permitted, but neither "
                          f"file exists yet under {args.pub_root} on the publisher.")
                    print("[state_relay] that is normal BEFORE the campaign starts.")
                    return 0
            except Exception as exc:
                now = time.time()
                msg = f"{type(exc).__name__}: {str(exc)[:120]}"
                if not ever_ok:
                    print(f"FAIL: {msg}")
                    if "permission denied" in msg.lower() or "pg_read_file" in msg:
                        print()
                        print("  The publisher role may not read server files. On the")
                        print("  PUBLISHER, as postgres:")
                        print("     GRANT pg_read_server_files TO <this role>;")
                        print("  or point R3_PUB_DSN at the postgres superuser.")
                    elif "does not exist" in msg.lower():
                        print()
                        print("  campaign_state.json is not there yet. That is normal")
                        print("  BEFORE the campaign starts - start the campaign on the")
                        print("  publisher first, or start this relay after it.")
                    if args.once:
                        return 2
                elif now - last_nag >= NAG_SEC:
                    last_nag = now
                    print(f"[state_relay] still failing - {msg}")
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                conn = None

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[state_relay] interrupted")
        return 0 if ever_ok else 1
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
