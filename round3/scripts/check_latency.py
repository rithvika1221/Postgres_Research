#!/usr/bin/env python3
"""
Measure the latency clumsy is ACTUALLY applying.  STANDALONE.  Read-only.

Run this on the SUBSCRIBER, before confirming each family F gate.

Why it exists
-------------
clumsy's own manual says its lag figure is "not intended to be an accurate
measure", and its hold buffer (KEEP_AT_MOST 2000 packets in src/lag.c) SENDS
held packets immediately when it fills rather than dropping them - so the
emulated latency can silently collapse with no error anywhere. The setting you
typed is not evidence. A measurement is.

`ping` cannot do this job: the clumsy filter matches only TCP packets from the
publisher's port 5432, and ICMP is not one of them, so ping reports the
undisturbed idle RTT no matter what clumsy is doing.

This opens ONE connection to the publisher (so connection setup is not counted)
and times many individual round trips over it. Those packets come back from
port 5432 and are therefore matched by exactly the same filter as the
replication stream.

  py check_latency.py --expect 0          # baseline, clumsy stopped
  py check_latency.py --expect 200        # after setting clumsy to 200 ms
  py check_latency.py --expect 50 --baseline 0.4

Exit codes
----------
  0  the measured added RTT matches --expect within tolerance
  1  it does not - do NOT run the level until this is explained
  2  could not measure
"""

import argparse
import os
import statistics as st
import sys
import time

import psycopg2

DEFAULT_PUB = os.environ.get("R3_PUB_DSN", "")

# A round trip that has to cross the injected delay once. SELECT 1 does no
# work on the server, so the time is transport plus a few microseconds.
PROBE_SQL = "SELECT 1"

# Discard this many at the start: the first queries on a new connection can
# include TLS/protocol warm-up and a cold code path.
WARMUP = 5

# How far the median may sit from the requested value before this fails.
# clumsy is a coarse instrument and the harness only needs to know the level
# is roughly right and, above all, NOT collapsed.
TOL_FRAC = 0.30
TOL_FLOOR_MS = 4.0


def measure(dsn, n, timeout):
    c = psycopg2.connect(dsn, connect_timeout=int(max(1, timeout)),
                         application_name="r3_latency_probe")
    c.set_session(autocommit=True)
    k = c.cursor()
    times = []
    for i in range(n + WARMUP):
        t0 = time.perf_counter()
        k.execute(PROBE_SQL)
        k.fetchone()
        dt = (time.perf_counter() - t0) * 1000.0
        if i >= WARMUP:
            times.append(dt)
    k.close()
    c.close()
    return times


def main():
    ap = argparse.ArgumentParser(
        description="Measure the RTT clumsy is actually adding (run on the subscriber).")
    ap.add_argument("--dsn", default=DEFAULT_PUB,
                    help="DSN of the PUBLISHER, from this machine. Defaults to "
                         "$R3_PUB_DSN, which on the subscriber must point at "
                         "the publisher's private address.")
    ap.add_argument("--expect", type=float, default=None,
                    help="the level's network_latency_ms (0 for the control). "
                         "Required unless --measure-only is given.")
    ap.add_argument("--measure-only", action="store_true",
                    help="report the RTT and stop. No pass/fail, no expectation. "
                         "This is the mode to use on a REMOTE-REGION subscriber, "
                         "where the RTT is the network's own and there is no "
                         "'correct' value to check it against. --expect 0 would "
                         "call a genuine 103 ms trans-Atlantic path a failure.")
    ap.add_argument("--region", default=None,
                    help="with --measure-only, print the ready-to-paste "
                         "make_region_family.py command for this region.")
    ap.add_argument("--baseline", type=float, default=None,
                    help="the median measured with clumsy stopped. Omit and "
                         "the F01 baseline is assumed to be negligible.")
    ap.add_argument("--samples", type=int, default=60)
    ap.add_argument("--timeout", type=float, default=15.0)
    args = ap.parse_args()

    if not args.dsn:
        print("FAIL: no DSN. Pass --dsn or set R3_PUB_DSN to the PUBLISHER.")
        return 2
    if args.expect is None and not args.measure_only:
        print("FAIL: pass --expect, or --measure-only to just report the RTT.")
        return 2

    try:
        times = measure(args.dsn, args.samples, args.timeout)
    except Exception as exc:
        print(f"FAIL: could not measure - {type(exc).__name__}: {str(exc)[:120]}")
        return 2
    if not times:
        print("FAIL: no samples")
        return 2

    times.sort()
    med = st.median(times)
    p95 = times[min(len(times) - 1, int(0.95 * len(times)))]
    base = args.baseline if args.baseline is not None else 0.0
    added = med - base

    print("=" * 64)
    print(f"ROUND-TRIP TIME TO THE PUBLISHER   ({len(times)} probes)")
    print("=" * 64)
    print(f"  min                {min(times):8.2f} ms")
    print(f"  median             {med:8.2f} ms")
    print(f"  p95                {p95:8.2f} ms")
    print(f"  max                {max(times):8.2f} ms")
    if args.baseline is not None:
        print(f"  baseline (given)   {base:8.2f} ms")

    if args.measure_only:
        iqr_lo = times[int(0.25 * len(times))]
        iqr_hi = times[min(len(times) - 1, int(0.75 * len(times)))]
        print(f"  IQR                {iqr_lo:8.2f} - {iqr_hi:.2f} ms")
        print()
        print("  MEASURED MEDIAN RTT (report this, do not round to a nominal value):")
        print(f"      {med:.1f} ms")
        print()
        if iqr_hi - iqr_lo > max(2.0, 0.15 * med):
            print("  NOTE: the interquartile spread is wide for a fixed network")
            print("  path. Re-run once; if it stays wide, record the spread in")
            print("  Methods alongside the median.")
            print()
        print("  Next, on the PUBLISHER:")
        reg = args.region if args.region else "REGION"
        print(f"      py make_region_family.py --region {reg} --rtt {med:.1f} "
              f"--vm-size THIS-VMS-SIZE --apply-mbps FROM-PREFLIGHT")
        print()
        print("  --vm-size and --apply-mbps are not optional decoration. The four")
        print("  subscribers are NOT the same Azure VM size (capacity forced that),")
        print("  so each region's size has to be recorded, and the apply-throughput")
        print("  headroom preflight_region.py measures is what shows the size")
        print("  difference did not carry the result. preflight_region.py prints")
        print("  this same command with both values already filled in - prefer it.")
        return 0

    print(f"  added RTT          {added:8.2f} ms      <- this is the measurement")
    print(f"  clumsy set to      {args.expect:8.2f} ms")
    print()

    tol = max(TOL_FLOOR_MS, args.expect * TOL_FRAC)
    if args.expect == 0:
        # The control. Anything substantial here means clumsy is still running.
        ok = med < 5.0
        print("PASS - no injected delay, this is a valid control level."
              if ok else
              f"FAIL - {med:.1f} ms is too slow for an uninjected link. Is clumsy "
              f"still started? Press Stop.")
        print(f"\nRecord this as the F01 baseline and pass it to later levels:"
              f"  --baseline {med:.2f}")
        return 0 if ok else 1

    if added < args.expect - tol:
        print(f"FAIL - only {added:.1f} ms of the {args.expect:.0f} ms asked for.")
        print()
        if added < args.expect * 0.25:
            print("  Almost nothing is being delayed. The filter is probably not")
            print("  matching. Check on this machine, while replication is running:")
            print("     netstat -ano | findstr \":5432\"")
            print("  and confirm the foreign address is the one in the filter, and")
            print("  that you used tcp.SrcPort (not DstPort).")
        else:
            print("  Some delay is arriving but well short of the setting. That is")
            print("  what clumsy's hold buffer looks like when it overflows: at")
            print("  KEEP_AT_MOST 2000 packets it SENDS the backlog rather than")
            print("  holding it. Lower the level's byte rate, or lower this RTT.")
        return 1

    if added > args.expect + tol:
        print(f"FAIL - {added:.1f} ms is more than the {args.expect:.0f} ms asked "
              f"for. Check the Lag value, and that only ONE direction is ticked.")
        return 1

    print(f"PASS - {added:.1f} ms measured against {args.expect:.0f} ms requested "
          f"(within {tol:.0f} ms).")
    print()
    print("Record the MEASURED value for Methods, not the requested one. Then")
    print("confirm the gate on the publisher:")
    print(f'    "{args.expect:.0f}" | Set-Content C:\\r3\\LATENCY_SET')
    print()
    print("This proves the link is delayed while idle. It does NOT prove the")
    print("buffer survives under load - for that, compare pg_stat_replication")
    print("write_lag against the F01 level once this one is running.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
