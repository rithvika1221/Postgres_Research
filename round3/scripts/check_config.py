#!/usr/bin/env python3
"""
Round 3 - compare a running PostgreSQL server against the study's specification.

    py check_config.py --role publisher
    py check_config.py --role subscriber --dsn "host=10.0.1.5 ..."
    py check_config.py --role subscriber --fix          apply, then restart

WHY
---
postgresql_settings.md says what the testbed should be. Nothing was checking
whether it IS. The Central US control subscriber turned out to be running
stock defaults - shared_buffers 128MB against the specified 8GB,
checkpoint_timeout 5min against 15min, track_io_timing off - while the eastus
subscriber, built by bootstrap_subscriber.ps1, had the full specified block.

Two subscribers differing by a factor of sixty-four in shared_buffers cannot
be described as "identical in every respect except distance". The difference
would not have appeared in any output: it would have appeared as a larger lag
at 0 ms than at 27 ms, and there would have been no way to explain it.

WHAT --fix DOES
---------------
Issues ALTER SYSTEM SET for every mismatch. That writes postgresql.auto.conf,
which takes precedence over postgresql.conf, so it corrects a server whatever
its conf file says and leaves the original file untouched. Settings whose
context is 'postmaster' need a restart; the script says exactly which, and
does NOT restart anything itself - restarting a database is the operator's
decision, not a script's.

capture_environment.py records the effective values afterwards, so the
manuscript cites what the server was actually running rather than what a
document said it should be.

EXIT CODES
----------
  0  matches the specification (or only 'recorded' settings differ)
  1  at least one blocking mismatch
  2  bad arguments, or could not connect
"""

import argparse
import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

import pg_spec

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""

SEV_COLOUR = {"physics": R, "monitor": R, "recorded": Y}


def audit(conn, role, spec=None):
    """Return [(name, expected, actual, severity, matches, context, note)]."""
    if spec is None:
        spec = pg_spec.load()[0][role]
    names = [n for n, *_ in spec]
    with conn.cursor() as cur:
        cur.execute("SELECT name, setting, unit, context, vartype "
                    "FROM pg_settings WHERE name = ANY(%s)", (names,))
        live = {r[0]: r[1:] for r in cur.fetchall()}

    out = []
    for name, expected, severity, note in spec:
        if name not in live:
            out.append((name, expected, "NOT PRESENT ON THIS SERVER",
                        severity, False, "", note))
            continue
        setting, unit, context, vartype = live[name]
        e, a = pg_spec.normalise(expected, setting, unit)
        if isinstance(e, float) and isinstance(a, float):
            matches = abs(e - a) < 1e-9
        else:
            matches = e == a
        out.append((name, expected, pg_spec.pretty(setting, unit),
                    severity, matches, context, note))
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Compare a running server against postgresql_settings.md")
    ap.add_argument("--role", required=True, choices=("publisher", "subscriber"))
    ap.add_argument("--dsn", default=None,
                    help="defaults to $R3_PUB_DSN or $R3_SUB_DSN for the role")
    ap.add_argument("--fix", action="store_true",
                    help="ALTER SYSTEM SET every mismatch, then reload. Says "
                         "which settings still need a restart; does not "
                         "restart anything itself.")
    ap.add_argument("--include-recorded", action="store_true",
                    help="with --fix, also correct 'recorded' settings "
                         "(default: yes for both, this flag is kept for "
                         "symmetry)")
    ap.add_argument("--only-blocking", action="store_true",
                    help="with --fix, correct only physics and monitor "
                         "settings and leave 'recorded' ones alone")
    a = ap.parse_args()

    dsn = a.dsn or os.environ.get(
        "R3_PUB_DSN" if a.role == "publisher" else "R3_SUB_DSN")
    if not dsn:
        print(f"no DSN. Pass --dsn, or set "
              f"{'R3_PUB_DSN' if a.role == 'publisher' else 'R3_SUB_DSN'}.")
        return 2

    try:
        conn = psycopg2.connect(dsn, connect_timeout=20,
                                application_name="r3_check_config")
        conn.autocommit = True
    except Exception as exc:
        print(f"cannot connect: {str(exc).strip().splitlines()[0]}")
        return 2

    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version'), "
                    "inet_server_addr()::text, current_user")
        ver, addr, who = cur.fetchone()

    spec_all, spec_src, spec_applied = pg_spec.load()
    rows = audit(conn, a.role, spec_all[a.role])
    bad = [r for r in rows if not r[4]]
    blocking = [r for r in bad if r[3] in pg_spec.BLOCKING]
    soft = [r for r in bad if r[3] not in pg_spec.BLOCKING]

    print(f"\n{B}Configuration audit - {a.role}{Z}")
    print(f"  server      PostgreSQL {ver} at {addr or 'local'} as {who}")
    print(f"  specified   postgresql_settings.md, {a.role} section "
          f"({len(rows)} settings)")
    pg_spec.announce(spec_src, spec_applied)
    print()
    print(f"  {'setting':<45} {'expected':>10}   {'actual':>10}   severity")
    print("  " + "-" * 88)
    for name, expected, actual, severity, matches, context, note in rows:
        if matches:
            print(f"  {G}OK{Z}   {name:<40} {expected:>10}   {actual:>10}")
        else:
            c = SEV_COLOUR[severity]
            print(f"  {c}DIFF{Z} {name:<40} {expected:>10}   {actual:>10}   "
                  f"{c}{severity}{Z}"
                  f"{'  (restart)' if context == 'postmaster' else ''}")

    if bad:
        print(f"\n{B}Why each mismatch matters{Z}")
        for name, expected, actual, severity, matches, context, note in bad:
            if note:
                print(f"\n  {name}  ({severity})")
                for line in _wrap(note, 72):
                    print(f"      {line}")
            else:
                print(f"\n  {name}  ({severity})   no further note")

    if not bad:
        print(f"\n  {G}This server matches the specification exactly.{Z}\n")
        return 0

    needs_restart = sorted({r[0] for r in bad if r[5] == "postmaster"})

    if a.fix:
        targets = [r for r in bad
                   if (r[3] in pg_spec.BLOCKING) or not a.only_blocking]
        print(f"\n{B}Applying {len(targets)} change(s){Z}")
        failed = 0
        for name, expected, actual, severity, matches, context, note in targets:
            try:
                with conn.cursor() as cur:
                    # ALTER SYSTEM takes a literal, and the parameter name
                    # cannot be a bind parameter, so the name is validated
                    # against the spec table above rather than interpolated
                    # from anything a caller supplied.
                    cur.execute(f'ALTER SYSTEM SET "{name}" = %s', (expected,))
                print(f"  {G}SET{Z}  {name} = {expected}")
            except Exception as exc:
                print(f"  {R}FAIL{Z} {name}: "
                      f"{str(exc).strip().splitlines()[0]}")
                failed += 1
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_reload_conf()")
            print(f"  {G}OK{Z}   configuration reloaded")
        except Exception as exc:
            print(f"  {R}FAIL{Z} reload: {exc}")
            failed += 1

        print(f"\n{B}Now restart this server.{Z}")
        if needs_restart:
            print("  These settings do not take effect until a restart:")
            for n in needs_restart:
                print(f"      {n}")
        else:
            print("  Nothing here needed a restart, but restart anyway so the")
            print("  captured environment and the running server cannot "
                  "disagree.")
        print()
        print("  On Windows, as administrator:")
        print("      Get-Service postgresql* | Restart-Service")
        print()
        print("  Then confirm, and record it:")
        print(f"      py check_config.py --role {a.role}")
        print(f"      py capture_environment.py --role {a.role} "
              f"--tag post-config-fix")
        print()
        return 1 if failed else 0

    print(f"\n{B}Result{Z}")
    if blocking:
        print(f"  {R}{len(blocking)} blocking mismatch(es){Z} - physics or "
              f"monitor. Do not run a campaign on this server.")
    if soft:
        print(f"  {Y}{len(soft)} recorded mismatch(es){Z} - will not move the "
              f"measurement, but the machines are not identically configured.")
    print()
    print("  Fix all of them:")
    print(f"      py check_config.py --role {a.role} --fix")
    print()
    print("  Or see the exact statements without running them:")
    for name, expected, actual, severity, matches, context, note in bad:
        print(f"      ALTER SYSTEM SET {name} = '{expected}';")
    print("      SELECT pg_reload_conf();")
    if needs_restart:
        print(f"\n  {', '.join(needs_restart)} need a server restart.")
    print()
    return 1 if blocking else 0


def _wrap(text, width):
    words, line, out = text.split(), "", []
    for w in words:
        if len(line) + len(w) + 1 > width:
            out.append(line)
            line = w
        else:
            line = f"{line} {w}".strip()
    if line:
        out.append(line)
    return out


if __name__ == "__main__":
    sys.exit(main())
