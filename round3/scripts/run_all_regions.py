#!/usr/bin/env python3
"""
Round 3 - run every region's campaign, in order, unattended. On the PUBLISHER.

    py run_all_regions.py --check        check everything, run nothing
    py run_all_regions.py --plan         per-region plans, change nothing
    py run_all_regions.py                do it

WHY A SEPARATE DRIVER
---------------------
run_region.py does one region properly and has been tested doing it. This does
not reimplement any of that - it runs run_region.py once per region, as a child
process, in the order the config gives.

What it adds is the thing you cannot get by running them one at a time: it
checks EVERY region before it starts ANY of them. Four regions is about an hour
and three quarters. Discovering at minute 75 that the third machine's monitor
was never started - and that its apply-side data is therefore gone - is the
failure this exists to prevent. Every check below is cheap, and all of them run
before the first byte of load.

It also forwards signals. Ctrl-C here stops the region in progress, which stops
its supervisor, which stops its orchestrator and load generators, and then this
stops rather than starting the next region.

EXIT CODES
----------
  0  every region completed and validated
  1  at least one region failed; the rest were not started
  2  the pre-run checks failed, so nothing was started
  130 interrupted
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

import run_region as RR

G = RR.G; R = RR.R; Y = RR.Y; B = RR.B; Z = RR.Z

_child = None


def _stop_child(signum, _frame):
    c = _child
    if c is not None and c.poll() is None:
        print(f"\n{Y}signal {signum} - stopping the region in progress "
              f"(pid {c.pid}){Z}")
        try:
            c.terminate()
            c.wait(timeout=180)
        except Exception:
            try:
                c.kill()
            except Exception:
                pass
    print("stopped. Nothing further was started.")
    raise SystemExit(130)


def head(t):
    print(f"\n{B}{t}{Z}")


def ok(n, d=""):
    print(f"  {G}OK{Z}    {n:<30} {d}")


def bad(n, d=""):
    print(f"  {R}FAIL{Z}  {n:<30} {d}")


def warn(n, d=""):
    print(f"  {Y}WARN{Z}  {n:<30} {d}")


def info(n, d=""):
    print(f"        {n:<30} {d}")


def helpers_up(cfg, name, password, pub):
    """Are THIS region's two helpers alive, right now?

    Checked again immediately before each region rather than only once at the
    start. A monitor can die during the twenty-five minutes the region before
    it was running - and run_region.py phase [8] would then refuse, correctly,
    but only after this driver had already moved on. Asking here means the
    message names the machine, the command and the resume line.
    """
    node = cfg["regions"][name]
    dsn, _ = RR.dsn_of(node, password)
    mon = relay = 0
    if dsn:
        try:
            c = RR.connect(dsn, "r3_runall_check")
            try:
                mon = RR.one(c, "SELECT count(*) FROM pg_stat_activity WHERE "
                                "application_name LIKE "
                                "'r3_monitor_subscriber%'")[0]
            finally:
                c.close()
        except Exception:
            return None, None
    host = str(node.get("host", ""))
    try:
        relays = [a for (a,) in RR.rows(pub,
                  "SELECT coalesce(host(client_addr),'local') "
                  "FROM pg_stat_activity "
                  "WHERE application_name LIKE 'r3_state_relay%%'")]
        relay = len([a for a in relays
                     if a == host or (a == "local" and host in
                                      ("127.0.0.1", "localhost", "::1"))])
    except Exception:
        relay = 0
    return mon, relay


def check_all(cfg, names, password, pub):
    """Every region, before any of them. Returns the number of blockers."""
    problems = 0

    head("[A] Every region answers, and has what it needs")
    for name in names:
        node = cfg["regions"][name]
        dsn, why = RR.dsn_of(node, password)
        if not dsn:
            bad(name, f"config: {why}")
            problems += 1
            continue
        try:
            c = RR.connect(dsn, "r3_runall_check")
        except Exception as exc:
            bad(name, f"unreachable - {str(exc).strip().splitlines()[0][:60]}")
            info("", "power the VM on, or take it out of 'order' in the config")
            problems += 1
            continue
        try:
            v = RR.one(c, "SELECT current_setting('server_version_num')::int")[0]
            if v // 10000 != 18:
                bad(name, f"PostgreSQL {v // 10000} - Round 3 is 18 only")
                problems += 1
            else:
                ok(name, f"reachable, PG {RR.one(c, 'SHOW server_version')[0]}")

            # The monitor, on THIS subscriber.
            n = RR.one(c, "SELECT count(*) FROM pg_stat_activity WHERE "
                          "application_name LIKE 'r3_monitor_subscriber%'")[0]
            if n:
                ok(f"  {name}: monitor", "running on that machine")
            else:
                bad(f"  {name}: monitor", "NOT running - no apply-side data "
                                          "would be recorded for this region")
                info("", f"on {node.get('vm', name)}:  .\\start_region_helpers.ps1 "
                         f"-Region {name}")
                problems += 1

            # The hardware record the manuscript needs.
            if not node.get("vm_size") or str(node["vm_size"]).upper().startswith("FILL"):
                bad(f"  {name}: vm_size", "not filled in - bash hw_inventory.sh")
                problems += 1
            if not node.get("apply_mbps"):
                warn(f"  {name}: apply_mbps", "not recorded. This is the number "
                                              "that makes the VM-size difference "
                                              "defensible; preflight_region.py "
                                              "measures it.")
        finally:
            c.close()

    head("[B] Each region's relay is connected to the publisher")
    try:
        relays = [a for (a,) in RR.rows(pub,
                  "SELECT coalesce(host(client_addr),'local') "
                  "FROM pg_stat_activity "
                  "WHERE application_name LIKE 'r3_state_relay%%'")]
    except Exception as exc:
        bad("pg_stat_activity", str(exc).strip().splitlines()[0][:60])
        relays = []
    for name in names:
        host = str(cfg["regions"][name].get("host", ""))
        mine = [a for a in relays
                if a == host or (a == "local" and host in ("127.0.0.1",
                                                           "localhost", "::1"))]
        if mine:
            ok(f"{name}: relay", f"connected from {mine[0]}")
        else:
            bad(f"{name}: relay", f"no r3_state_relay from {host}")
            info("", "without it that machine labels every sample 'pending'")
            info("", f"on that VM:  .\\start_region_helpers.ps1 -Region {name}")
            problems += 1

    head("[C] Only one subscriber will be fed at a time")
    try:
        senders = RR.rows(pub, "SELECT application_name, "
                               "coalesce(host(client_addr),'local'), state "
                               "FROM pg_stat_replication")
        slots = RR.rows(pub, "SELECT slot_name, active FROM "
                             "pg_replication_slots WHERE slot_type='logical'")
    except Exception:
        senders, slots = [], []
    if len(senders) > 1:
        bad("walsenders", f"{len(senders)} standbys are being fed RIGHT NOW: "
                          f"{[s[1] for s in senders]}")
        info("", "py quiesce_regions.py --disable")
        problems += 1
    else:
        ok("walsenders", f"{len(senders)} - "
                         f"{senders[0][1] if senders else 'none attached'}")
    info("logical slots", f"{[s[0] for s in slots] or 'none'} - phase [4] of "
                          f"each region rebuilds this")

    return problems


def main():
    ap = argparse.ArgumentParser(
        description="Run every region's campaign in order, from the publisher.")
    ap.add_argument("--config", default=RR.CONFIG)
    ap.add_argument("--regions", default=None,
                    help="comma-separated subset, in the order given. "
                         "Default: the config's 'order' list.")
    ap.add_argument("--check", action="store_true",
                    help="run the pre-run checks and stop")
    ap.add_argument("--plan", action="store_true",
                    help="pre-run checks, then each region's plan. Changes "
                         "nothing.")
    ap.add_argument("--keep-going", action="store_true",
                    help="carry on to the next region after a failure. Off by "
                         "default: a failure usually means something is wrong "
                         "with the testbed, not with that one region.")
    ap.add_argument("--out", default=None)
    args, extra = ap.parse_known_args()
    extra = [a for a in extra if a != "--"]

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        s = getattr(signal, name, None)
        if s is not None:
            try:
                signal.signal(s, _stop_child)
            except (ValueError, OSError):
                pass

    cfg = RR.load_config(args.config)
    names = ([n.strip() for n in args.regions.split(",") if n.strip()]
             if args.regions else (cfg.get("order") or list(cfg["regions"])))
    unknown = [n for n in names if n not in cfg["regions"]]
    if unknown:
        print(f"not in {args.config}: {', '.join(unknown)}")
        return 2

    password = os.environ.get("R3_PGPASSWORD")
    if not password:
        import re
        m = re.search(r"password=(\S+)", os.environ.get("R3_PUB_DSN", ""))
        password = m.group(1) if m else None
    if not password:
        print('no password. setx R3_PGPASSWORD "..." /M, then a NEW window.')
        return 2

    root = cfg["publisher"].get("root") or "C:\\r3"
    out = args.out or os.environ.get("R3_DATA") or os.path.join(root, "data")

    print(f"\n{B}Round 3 - all regions, in order{Z}")
    print(f"  {'  ->  '.join(names)}")
    print(f"  config {args.config}")
    print(f"  data   {out}")
    print(f"  about {25 * len(names)} minutes, unattended")

    pdsn, why = RR.dsn_of(cfg["publisher"], password)
    if not pdsn:
        print(f"publisher: {why}")
        return 2
    try:
        pub = RR.connect(pdsn, "r3_runall")
    except Exception as exc:
        print(f"\n{R}cannot reach the publisher: "
              f"{str(exc).strip().splitlines()[0]}{Z}\n")
        return 2

    problems = check_all(cfg, names, password, pub)

    # phase_busy prints its own heading.
    # plan=True so it reports rather than exiting; it returns False when it
    # found something, and that is a blocker here exactly as it is there.
    try:
        if not RR.phase_busy(pub, True, root, out):
            problems += 1
    except SystemExit:
        problems += 1

    print("\n" + "=" * 78)
    if problems:
        print(f"{R}{problems} blocking problem(s) - nothing was started.{Z}")
        print("=" * 78)
        print("\n  Fix them and run this again. Every one of these would have")
        print("  cost you a region part way through the sequence.\n")
        return 2
    print(f"{G}All {len(names)} region(s) are ready.{Z}")
    print("=" * 78)

    if args.check:
        print("\n  --check only. Nothing was run.")
        print(f"  Start it:   py run_all_regions.py\n")
        return 0

    results = []
    t_all = time.time()
    for i, name in enumerate(names, 1):
        head(f"=== region {i} of {len(names)}: {name} ===")
        if not args.plan:
            mon, relay = helpers_up(cfg, name, password, pub)
            if mon is None:
                bad(f"{name}", "has stopped answering since the checks above")
                info("", "power it on, then:  py run_all_regions.py --regions "
                         + ",".join(names[i - 1:]))
                results.append((name, 90, 0.0))
                break
            if not mon or not relay:
                bad(f"{name}: helpers", f"monitor={mon} relay={relay} - one or "
                                        f"both died while an earlier region ran")
                info("", f"on {cfg['regions'][name].get('vm', name)}:")
                info("", f"   .\\start_region_helpers.ps1 -Region {name}")
                info("", "then pick up where this stopped:")
                info("", "   py run_all_regions.py --regions "
                         + ",".join(names[i - 1:]))
                results.append((name, 91, 0.0))
                break
            ok(f"{name}: helpers", f"monitor and relay both alive")

        cmd = [sys.executable, os.path.join(HERE, "run_region.py"),
               "--region", name, "--config", args.config, "--out", out]
        if args.plan:
            cmd.append("--plan")
        cmd += extra
        print(f"  {' '.join(cmd[1:])}\n")
        t0 = time.time()
        global _child
        proc = subprocess.Popen(cmd)
        _child = proc
        try:
            proc.wait()
        finally:
            _child = None
        mins = (time.time() - t0) / 60.0
        results.append((name, proc.returncode, mins))
        if proc.returncode != 0 and not args.plan:
            print(f"\n{R}{name} exited {proc.returncode} after "
                  f"{mins:.0f} min.{Z}")
            if not args.keep_going:
                print(f"{Y}Stopping here rather than starting the next region."
                      f"{Z}")
                print("  A region failure is usually the testbed, not that one")
                print("  region, and running three more would multiply it.")
                print(f"  Resume this one:  py run_region.py --region {name} "
                      f"--resume")
                print(f"  Or carry on anyway: py run_all_regions.py --regions "
                      f"{','.join(names[i:])}")
                break

    print("\n" + "=" * 78)
    print(f"{B}Summary{Z}      total {(time.time() - t_all) / 60.0:.0f} min")
    print("  region                     exit   minutes")
    print("  " + "-" * 44)
    for name, rc, mins in results:
        mark = f"{G}ok{Z}" if rc == 0 else f"{R}FAILED{Z}"
        print(f"  {name:<26} {rc:>4}   {mins:>6.0f}   {mark}")
    notrun = [n for n in names if n not in [r[0] for r in results]]
    for n in notrun:
        print(f"  {n:<26}    -        -   not started")
    print("=" * 78)

    failed = [r for r in results if r[1] != 0]
    if args.plan:
        print("\n  PLAN ONLY - nothing was changed.\n")
        return 0
    if failed or notrun:
        print("\n  Read the failing region's run_report before re-running "
              "anything.\n")
        return 1
    print(f"\n{G}Every region completed and validated.{Z}")
    print("\n  Before deallocating any VM, confirm its apply-side series "
          "arrived:")
    print(f"      Get-ChildItem {out}\\subscriber_F_lat_*_rep*.csv | "
          f"Select-Object Name, Length")
    print("\n  Then the comparison:")
    for n in names:
        print(f"      py analyse_run.py --run-id F_lat_{n}_rep1")
    print("\n  No level may be attributed to SUBSCRIBER apply. If none is,")
    print("  the subscriber was never the limiting resource and the VM-size")
    print("  differences cannot account for the lag differences.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted\n")
        sys.exit(130)
