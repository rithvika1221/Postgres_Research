#!/usr/bin/env python3
"""
Round 3 - make it safe to power on the region subscribers. Run on the PUBLISHER.

    py quiesce_regions.py                 read-only: who is attached, who could be
    py quiesce_regions.py --disable       disable mysub on EVERY reachable region
    py quiesce_regions.py --region eastus --enable    re-enable exactly one

WHY THIS EXISTS
---------------
The three remote subscribers are powered off, and each one still carries the
subscription it was left with. Every one of them is named `mysub` and every one
of them asks for the publisher's slot `mysub`. PostgreSQL will only let one
walsender hold that slot, so they cannot all succeed - but that is luck, not
design, and it decides the winner by whoever asks at the right moment.

Two ways that hurts:

  * Boot a region VM while another subscriber holds the slot and the new one
    retries in a loop, filling the publisher's log and holding a connection
    slot. Harmless until the holder blips - then the wrong machine can take it.

  * Boot a region VM during a campaign and, if it ever does win the slot, the
    publisher is feeding a different standby than the one being measured. The
    harness matches on application_name='mysub', which every one of them uses,
    so nothing in the output would say so.

`run_region.py` phase [4] drops every subscription it can reach, so the steady
state during a region run is correct. This closes the window BEFORE that: power
the machines on, run this once, and no subscriber can claim the slot until a
region run deliberately gives it one.

Disabling is the right verb, not dropping. A disabled subscription keeps its
identity and can be re-enabled; dropping it on a machine you then power off
means rebuilding it later with the conninfo typed again.

EXIT CODES
----------
  0  nothing to do, or the change was made
  1  at least one region could not be reached or could not be changed
  2  bad arguments or no config
"""

import argparse
import json
import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

HERE = os.path.dirname(os.path.abspath(__file__))
def _find_config():
    """Where the region map actually is.

    run_region.py --init writes regions.json. Two scripts here were written
    against the name "region_map.json" instead, which meant --init produced a
    file they then refused to read. Rather than pick a winner and leave anyone
    holding the other name, look for both - newest first if somehow both exist.
    """
    names = ["regions.json", "region_map.json"]
    found = [os.path.join(HERE, n) for n in names
             if os.path.isfile(os.path.join(HERE, n))]
    if found:
        found.sort(key=os.path.getmtime, reverse=True)
        return found[0]
    return os.path.join(HERE, names[0])


CONFIG = _find_config()

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""


def ok(n, d=""):
    print(f"  {G}OK{Z}    {n:<26} {d}")


def bad(n, d=""):
    print(f"  {R}FAIL{Z}  {n:<26} {d}")


def warn(n, d=""):
    print(f"  {Y}WARN{Z}  {n:<26} {d}")


def info(n, d=""):
    print(f"        {n:<26} {d}")


def dsn_of(node, password):
    if not node.get("host") or str(node["host"]).upper().startswith("FILL"):
        return None
    parts = [f"host={node['host']}", f"port={node.get('port', 5432)}",
             f"dbname={node.get('dbname', 'sub')}",
             f"user={node.get('user', 'postgres')}"]
    if password:
        parts.append(f"password={password}")
    parts.append("connect_timeout=10")
    return " ".join(parts)


def main():
    ap = argparse.ArgumentParser(
        description="Stop powered-on region subscribers racing for the "
                    "publisher's replication slot.")
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--disable", action="store_true",
                    help="disable mysub on every reachable region")
    ap.add_argument("--enable", action="store_true",
                    help="enable mysub. Requires --region: enabling more than "
                         "one is the thing this script exists to prevent.")
    ap.add_argument("--region", default=None,
                    help="act on one region only")
    ap.add_argument("--include-control", action="store_true",
                    help="also act on the publisher's own region. Off by "
                         "default: the control subscriber is the one that "
                         "carries families A-E and disabling it by accident "
                         "is how a campaign dies quietly.")
    a = ap.parse_args()

    if a.enable and not a.region:
        print("--enable needs --region. Enabling every subscription at once "
              "is exactly what this script prevents.")
        return 2
    if a.enable and a.disable:
        print("--enable and --disable are opposites. Pick one.")
        return 2

    if not os.path.exists(a.config):
        print(f"no {a.config}.  Create it with:  py run_region.py --init")
        return 2
    with open(a.config, encoding="utf-8") as fh:
        cfg = json.load(fh)

    password = os.environ.get("R3_PGPASSWORD")
    if not password:
        import re
        m = re.search(r"password=(\S+)", os.environ.get("R3_PUB_DSN", ""))
        password = m.group(1) if m else None
    if not password:
        print('no password. setx R3_PGPASSWORD "..." /M, then a NEW window.')
        return 2

    control = (cfg.get("publisher", {}) or {}).get("azure_region")
    regions = cfg.get("regions", {})
    names = [a.region] if a.region else list(cfg.get("order") or regions)
    unknown = [n for n in names if n not in regions]
    if unknown:
        print(f"not in {a.config}: {', '.join(unknown)}")
        return 2

    mode = "disable" if a.disable else ("enable" if a.enable else "report")
    print(f"\n{B}Round 3 region subscriptions - {mode}{Z}")
    print(f"  config {a.config}")
    if control:
        print(f"  control region '{control}'"
              f"{'' if a.include_control else ' - skipped unless --include-control'}")

    # --- who holds the publisher's slot right now? ------------------------
    print(f"\n{B}The publisher's slot{Z}")
    pub_dsn = os.environ.get("R3_PUB_DSN")
    if pub_dsn:
        try:
            pc = psycopg2.connect(pub_dsn, connect_timeout=10,
                                  application_name="r3_quiesce")
            pc.autocommit = True
            with pc.cursor() as cur:
                cur.execute("SELECT slot_name, active, active_pid, wal_status "
                            "FROM pg_replication_slots WHERE slot_type='logical'")
                slots = cur.fetchall()
                cur.execute("SELECT application_name, "
                            "coalesce(host(client_addr),'local'), state "
                            "FROM pg_stat_replication")
                senders = cur.fetchall()
            for sn, act, pid, ws in slots:
                (ok if act else info)(f"slot {sn}",
                                      f"active={act} pid={pid} wal_status={ws}")
            if not slots:
                info("slot", "none - no subscription is attached")
            for appname, addr, state in senders:
                ok("walsender", f"{appname} from {addr}, {state}")
            if len(senders) > 1:
                bad("more than one walsender",
                    "the publisher is feeding more than one standby RIGHT NOW")
        except Exception as exc:
            warn("publisher", str(exc).strip().splitlines()[0][:70])
    else:
        info("R3_PUB_DSN", "not set - skipping the publisher's own view")

    # --- each region ------------------------------------------------------
    print(f"\n{B}Regions{Z}")
    problems = 0
    changed = 0
    for name in names:
        node = regions[name]
        if name == control and not a.include_control and not a.region:
            info(name, "control region, skipped")
            continue
        dsn = dsn_of(node, password)
        if not dsn:
            warn(name, "no host in the config yet")
            continue
        try:
            c = psycopg2.connect(dsn, connect_timeout=10,
                                 application_name="r3_quiesce")
            c.autocommit = True
        except Exception as exc:
            first = str(exc).strip().splitlines()[0]
            # Powered off is the expected state here, not a problem.
            info(name, f"unreachable ({first[:56]}) - powered off, nothing "
                       f"to do")
            continue
        try:
            with c.cursor() as cur:
                cur.execute("SELECT subname, subenabled FROM pg_subscription")
                subs = cur.fetchall()
                if not subs:
                    ok(name, "no subscription on this machine")
                elif mode == "report":
                    for sn, en in subs:
                        (warn if en else ok)(
                            name, f"subscription '{sn}' "
                                  f"{'ENABLED - it will try to claim the slot' if en else 'disabled'}")
                else:
                    want = (mode == "enable")
                    for sn, en in subs:
                        if en == want:
                            ok(name, f"'{sn}' already {'enabled' if want else 'disabled'}")
                            continue
                        cur.execute(f'ALTER SUBSCRIPTION {sn} '
                                    f'{"ENABLE" if want else "DISABLE"}')
                        changed += 1
                        ok(name, f"'{sn}' {'ENABLED' if want else 'DISABLED'}")
        except Exception as exc:
            bad(name, str(exc).strip().splitlines()[0][:70])
            problems += 1
        finally:
            c.close()

    print("\n" + "=" * 74)
    if mode == "report":
        print("Read-only. Nothing was changed.")
        print("  To stop every reachable region claiming the slot:")
        print("      py quiesce_regions.py --disable")
    else:
        print(f"{changed} subscription(s) changed.")
        if mode == "disable":
            print("  Every reachable region is now inert. run_region.py will")
            print("  drop and recreate the one it is given, so you do not need")
            print("  to enable anything by hand:")
            print("      py run_region.py --region eastus --plan")
    if problems:
        print(f"{R}{problems} region(s) could not be changed - read the FAIL "
              f"lines above.{Z}")
    print("=" * 74 + "\n")
    return 1 if problems else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted\n")
        sys.exit(1)
