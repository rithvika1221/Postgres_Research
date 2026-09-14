#!/usr/bin/env python3
"""
Round 3 - what helpers are running on THIS subscriber? Run on a SUBSCRIBER.

    py check_helpers.py

Reads only. Answers one question: is there EXACTLY ONE subscriber monitor and
EXACTLY ONE state relay belonging to this machine?

Why it matters: monitor.py opens a single connection and APPENDS to its CSV.
Two monitors interleave two sample streams in one file, and validate_run.py
cannot separate them afterwards - the run looks fine and its apply-side series
is nonsense. Closing an RDP window does not stop a helper; only the STOP file
does, so extras accumulate quietly.

This exists because the equivalent one-liner is unquotable in PowerShell:
single quotes and commas are eaten before Python ever sees the string.
"""

import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""


def rows(dsn, sql, args=None):
    c = psycopg2.connect(dsn, connect_timeout=10,
                         application_name="r3_check_helpers")
    try:
        with c.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        c.close()


def main():
    problems = 0
    print(f"\n{B}Round 3 - helpers on this machine{Z}")

    sub = os.environ.get("R3_SUB_DSN")
    pub = os.environ.get("R3_PUB_DSN")
    if not sub:
        print("  R3_SUB_DSN is not set. Open a NEW window after setx.")
        return 2

    # ---- the monitor, on this machine's own server --------------------
    print(f"\n{B}Subscriber monitor{Z}   (local server)")
    try:
        mons = rows(sub, "SELECT pid, application_name, "
                         "to_char(backend_start,'YYYY-MM-DD HH24:MI:SS'), state "
                         "FROM pg_stat_activity "
                         "WHERE application_name LIKE 'r3_monitor%' "
                         "ORDER BY backend_start")
    except Exception as exc:
        print(f"  {R}cannot reach this machine's PostgreSQL{Z}: "
              f"{str(exc).strip().splitlines()[0]}")
        return 2
    for pid, app, started, state in mons:
        print(f"    pid {pid:<8} {app:<26} started {started}   {state}")
    if len(mons) == 1:
        print(f"  {G}OK{Z}    exactly one monitor")
    elif not mons:
        print(f"  {R}FAIL{Z}  no monitor running - no apply-side data would be "
              f"recorded")
        problems += 1
    else:
        print(f"  {R}FAIL{Z}  {len(mons)} monitors. They all append to the SAME "
              f"CSV.")
        problems += 1

    # ---- the relay, seen from the publisher, FROM THIS MACHINE --------
    print(f"\n{B}State relay{Z}   (as the publisher sees it, from here only)")
    if not pub:
        print(f"  {R}FAIL{Z}  R3_PUB_DSN is not set on this machine - "
              f"state_relay.py needs it")
        problems += 1
    else:
        try:
            rel = rows(pub, "SELECT pid, application_name, "
                            "to_char(backend_start,'YYYY-MM-DD HH24:MI:SS'), "
                            "coalesce(host(client_addr),'local') "
                            "FROM pg_stat_activity "
                            "WHERE application_name LIKE 'r3_state_relay%' "
                            "AND client_addr IS NOT DISTINCT FROM "
                            "inet_client_addr() "
                            "ORDER BY backend_start")
            others = rows(pub, "SELECT count(*) FROM pg_stat_activity "
                               "WHERE application_name LIKE 'r3_state_relay%' "
                               "AND client_addr IS DISTINCT FROM "
                               "inet_client_addr()")[0][0]
        except Exception as exc:
            print(f"  {R}cannot reach the publisher{Z}: "
                  f"{str(exc).strip().splitlines()[0]}")
            return 2
        for pid, app, started, addr in rel:
            print(f"    pid {pid:<8} {app:<26} started {started}   from {addr}")
        if len(rel) == 1:
            print(f"  {G}OK{Z}    exactly one relay from this machine")
        elif not rel:
            print(f"  {R}FAIL{Z}  no relay from this machine - every sample "
                  f"would be labelled 'pending'")
            problems += 1
        else:
            print(f"  {R}FAIL{Z}  {len(rel)} relays from this machine")
            problems += 1
        print(f"        {others} relay(s) from OTHER machines - expected, "
              f"and not this machine's business")

    # ---- the state file the monitor actually reads --------------------
    root = os.environ.get("R3_ROOT") or r"C:\r3"
    state = os.path.join(root, "campaign_state.json")
    print(f"\n{B}campaign_state.json{Z}   (written here by the relay)")
    if os.path.isfile(state):
        import time
        age = time.time() - os.path.getmtime(state)
        if age < 120:
            print(f"  {G}OK{Z}    {state}, written {age:.0f}s ago")
        else:
            print(f"  {Y}WARN{Z}  {state}, last written {age/60:.0f} min ago - "
                  f"the relay may be stuck")
    else:
        print(f"  {R}FAIL{Z}  {state} does not exist - the relay has never "
              f"written it")
        problems += 1

    print("\n" + "=" * 70)
    if problems:
        print(f"{R}{problems} problem(s).{Z}  Clean up and start fresh:")
        print(f"\n    New-Item {os.path.join(root, 'STOP')} -ItemType File")
        print("    (wait for EVERY helper window to close, then run this again")
        print("     - it must show zero monitors and zero relays)")
        print(f"    Remove-Item {os.path.join(root, 'STOP')} "
              f"-ErrorAction SilentlyContinue")
        print("    .\\start_region_helpers.ps1 -Region REGIONNAME\n")
        print("  If a python process survives the STOP file, kill it by the")
        print("  pid listed above:   Stop-Process -Id PID -Force\n")
        return 1
    print(f"{G}One monitor, one relay, state file fresh. This machine is "
          f"ready.{Z}\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(1)
