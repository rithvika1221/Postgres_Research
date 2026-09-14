#!/usr/bin/env python3
"""
Round 3 - copy the subscriber's monitor CSVs to the publisher. Run on the
PUBLISHER, after a campaign.

    py collect_subscriber.py --dry-run
    py collect_subscriber.py

WHY THIS EXISTS
---------------
validate_run.py reads publisher_<run>.csv AND subscriber_<run>.csv from ONE
directory - the publisher's --out. The subscriber monitor writes its CSV on
the SUBSCRIBER. Nothing moves it across.

run_region.py does this itself for the region families. start_campaign.ps1
does not, so for families A-E the subscriber's samples stay on the subscriber
and validate_run.py reports

    ! no subscriber CSV at C:\\r3\\data\\subscriber_<run>.csv

as a WARNING and still says "PASS with warnings - usable". That is the worst
shape a data-loss bug can take: every run passes, and every apply-side series
- rows_applied_per_sec, apply worker count, subscriber CPU and disk - is
missing, discovered during analysis.

There is no file share between the two machines, so the transport is the one
channel that certainly works: pg_read_file over port 5432, the same mechanism
state_relay.py uses in the other direction.

WHAT IT GUARDS AGAINST
----------------------
  * copying a file the monitor is still writing - it waits for each size to
    stop changing, because the last samples of a run are the drain samples
  * a short copy reported as success - the byte count is verified against
    pg_stat_file, and a mismatch is a failure, not a warning
  * one region's subscriber_pending.csv overwriting another's - files not
    named after a run get the source host in their name

EXIT CODES
----------
  0  everything copied (or nothing to copy)
  1  at least one file failed to copy
  2  bad arguments, or the subscriber could not be reached
"""

import argparse
import os
import re
import sys
import time

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""

READ_CHUNK = 1 << 20
SETTLE_TRIES = 20
SETTLE_GAP = 1.0

RUNID = re.compile(r"^subscriber_(?P<rid>.+?)(?:_events)?\.(?:csv|log)$")
TRANSIENT = re.compile(r"^subscriber_(?:pending|idle|startup|adhoc)"
                       r"(?:_events)?\.(?:csv|log)$")


def one(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


def rows(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def settle(conn, remote):
    """Wait until a remote file's size stops changing, then return it."""
    last = -1
    for _ in range(SETTLE_TRIES):
        size = one(conn, "SELECT (pg_stat_file(%s)).size", (remote,))[0]
        if size == last:
            return size
        last = size
        time.sleep(SETTLE_GAP)
    return last


def main():
    ap = argparse.ArgumentParser(
        description="Copy the subscriber's monitor CSVs to the publisher.")
    ap.add_argument("--sub-dsn", default=os.environ.get("R3_SUB_DSN"),
                    help="defaults to $R3_SUB_DSN")
    ap.add_argument("--sub-root", default=r"C:\r3",
                    help="where C:\\r3 lives on the SUBSCRIBER")
    ap.add_argument("--out", default=os.environ.get("R3_DATA") or r"C:\r3\data",
                    help="the publisher's data directory. Default $R3_DATA")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be copied and copy nothing")
    ap.add_argument("--tag", default=None,
                    help="name to give files that are not named after a run "
                         "(pending/idle/startup). Default: the subscriber's "
                         "hostname, so four machines cannot collide.")
    ap.add_argument("--overwrite", action="store_true",
                    help="replace a file that already exists here. Without "
                         "this, an existing file is left alone and reported.")
    a = ap.parse_args()

    if not a.sub_dsn:
        print("no subscriber DSN. Pass --sub-dsn, or set R3_SUB_DSN.")
        return 2
    if not os.path.isdir(a.out):
        print(f"--out {a.out} does not exist")
        return 2

    try:
        c = psycopg2.connect(a.sub_dsn, connect_timeout=20,
                             application_name="r3_collect")
        c.autocommit = True
    except Exception as exc:
        print(f"cannot reach the subscriber: "
              f"{str(exc).strip().splitlines()[0]}")
        return 2

    # host() strips the /32 that inet_server_addr()::text carries, so the
    # tag reads subscriber_10.0.1.5_pending.csv rather than
    # subscriber_10.0.1.5_32_pending.csv.
    host = one(c, "SELECT host(inet_server_addr()), "
                  "current_setting('cluster_name', true)")
    tag = a.tag or re.sub(r"[^A-Za-z0-9.\-]", "_",
                          str(host[0] or "sub"))

    if not one(c, "SELECT pg_has_role(current_user, 'pg_read_server_files', "
                  "'USAGE') OR (SELECT rolsuper FROM pg_roles WHERE "
                  "rolname = current_user)")[0]:
        print("this login can neither read server files nor is it superuser; "
              "cannot copy anything")
        return 2

    sdata = (a.sub_root.rstrip("\\/") + "/data").replace("\\", "/")
    print(f"\n{B}Collecting from the subscriber{Z}")
    print(f"  source      {sdata}  on {host[0] or 'the subscriber'}")
    print(f"  destination {a.out}")
    if a.dry_run:
        print(f"  {Y}dry run - nothing will be written{Z}")

    try:
        listing = [n for (n,) in rows(c, "SELECT pg_ls_dir(%s)", (sdata,))]
    except Exception as exc:
        print(f"\n  cannot list {sdata}: "
              f"{str(exc).strip().splitlines()[0]}")
        print("  Check the subscriber monitor's --out really is that folder.")
        return 2

    want = sorted(n for n in listing
                  if n.startswith("subscriber_") and
                  (n.endswith(".csv") or n.endswith(".log")))
    if not want:
        print(f"\n  {R}No subscriber_* files in {sdata}.{Z}")
        print("  The subscriber monitor never wrote any, so there is no")
        print("  apply-side data for this campaign. On the subscriber:")
        print("      cd C:\\r3\\scripts ;  .\\start_subscriber_monitor.ps1")
        print(f"  Files there: {', '.join(sorted(listing)[:12]) or 'none'}\n")
        return 1

    copied = failed = skipped = 0
    print()
    for name in want:
        remote = f"{sdata}/{name}"
        # A run-id-named file is unique across machines. pending/idle/startup
        # are not - every subscriber writes subscriber_pending.csv - so those
        # get the source host in the name.
        local = name if not TRANSIENT.match(name) else \
            name.replace("subscriber_", f"subscriber_{tag}_", 1)
        dest = os.path.join(a.out, local)

        try:
            if a.dry_run:
                size = one(c, "SELECT (pg_stat_file(%s)).size", (remote,))[0]
                exists = " (already here)" if os.path.exists(dest) else ""
                print(f"  {B}COPY{Z}  {local:<46} {size:>12,} bytes{exists}")
                continue

            if os.path.exists(dest) and not a.overwrite:
                print(f"  {Y}SKIP{Z}  {local:<46} already here "
                      f"(--overwrite to replace)")
                skipped += 1
                continue

            # A run's CSV stops growing when the monitor rotates off that
            # run, so waiting for its size to settle is exactly right. The
            # pending/idle/startup files are the monitor's LIVE files and
            # never stop growing while it runs, so settling them just burns
            # 20 seconds each and still copies a snapshot. Take the snapshot.
            if TRANSIENT.match(name):
                size = one(c, "SELECT (pg_stat_file(%s)).size",
                           (remote,))[0]
            else:
                size = settle(c, remote)
            buf, off = [], 0
            while off < size:
                chunk = one(c, "SELECT pg_read_file(%s, %s, %s)",
                            (remote, off, min(READ_CHUNK, size - off)))[0]
                if not chunk:
                    break
                buf.append(chunk)
                off += len(chunk.encode("utf-8", "replace"))
            text = "".join(buf)
            got = len(text.encode("utf-8", "replace"))
            if got != size:
                print(f"  {R}SHORT{Z} {local:<46} got {got:,} of {size:,} "
                      f"bytes - NOT written")
                failed += 1
                continue
            with open(dest, "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
            lines = text.count("\n")
            print(f"  {G}OK{Z}    {local:<46} {size:>12,} bytes, "
                  f"{lines:,} lines")
            copied += 1
        except Exception as exc:
            print(f"  {R}FAIL{Z}  {local:<46} "
                  f"{str(exc).strip().splitlines()[0]}")
            failed += 1

    if a.dry_run:
        print(f"\n  {len(want)} file(s) would be copied. Nothing was "
              f"written.\n")
        return 0

    print()
    print(f"  copied {copied}, skipped {skipped}, failed {failed}")

    # The point of all this is that validate_run.py can see both CSVs. Say
    # plainly which runs now have an apply-side series and which do not.
    runs = {}
    for n in os.listdir(a.out):
        m = re.fullmatch(r"manifest_(.+)\.json", n)
        if m:
            rid = m.group(1)
            runs[rid] = os.path.isfile(
                os.path.join(a.out, f"subscriber_{rid}.csv"))
    if runs:
        have = [r for r, v in runs.items() if v]
        miss = [r for r, v in runs.items() if not v]
        print()
        print(f"  runs with a subscriber CSV here:    {len(have)}")
        if miss:
            print(f"  {Y}runs WITHOUT one:{Z}                   {len(miss)}")
            for r in sorted(miss):
                print(f"      {r}")
            print("  validate_run.py will warn on those and still say PASS,")
            print("  so they are easy to miss. If the subscriber monitor was")
            print("  not running during them, that data does not exist.")

    if failed:
        print(f"\n  {R}Do not deallocate the subscriber yet.{Z}\n")
        return 1
    print(f"\n  Re-validate so the reports include the apply side:")
    print(f"      Get-ChildItem {a.out}\\manifest_*.json | ForEach-Object "
          f"{{ py validate_run.py --run-id "
          f"($_.BaseName -replace '^manifest_','') }}\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted\n")
        sys.exit(1)
