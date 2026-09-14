#!/usr/bin/env python3
"""
Measure the SUSTAINED throughput of the injected-latency path.  STANDALONE.
Read-only.  Run on the SUBSCRIBER.

Why it exists, and why it measures for a fixed TIME
---------------------------------------------------
check_latency.py answers "is the delay there?". It cannot answer "can this
path still move 6 MB/s while the delay is there?", and on this testbed those
questions had very different answers: family F's 100 ms level sustained about
0.4 MB/s against 6 MB/s offered, with write_lag equal to replay_lag, meaning
the subscriber sat idle waiting for bytes.

The first version of this tool transferred a fixed number of megabytes and
divided by the elapsed time. That was wrong. At 200 ms RTT a 2 MB transfer is
a handful of round trips, so it measures TCP slow start and nothing else - it
reported 5.36 MB/s for a path that actually sustained 0.4, and reported 200 ms
as FASTER than 150 ms, which is impossible and was the clue.

So this streams for a fixed DURATION and reports the rate over the tail, after
the ramp is over. The tail rate is the number family F has to live under.

  py check_throughput.py --label "clumsy stopped"
  py check_throughput.py --label "Lag 69 (100 ms)" --seconds 30

Exit codes
----------
  0  measured
  1  measured, but the result is not trustworthy - read the warnings
  2  could not measure
"""

import argparse
import io
import os
import statistics as st
import time

import psycopg2

DEFAULT_PUB = os.environ.get("R3_PUB_DSN", "")

ROW_BYTES = 1000

# Enough rows that we always stop on the clock, never because the data ran
# out. 8 GB at 1 kB a row.
MAX_ROWS = 8_000_000

# Ignore this fraction of the run as ramp-up, and report the rest.
RAMP_FRACTION = 0.5

SAMPLE_INTERVAL = 0.25


class StopTransfer(Exception):
    pass


class Sink(io.RawIOBase):
    """Counts bytes, samples the rate over time, and stops on the clock."""

    def __init__(self, seconds):
        self.n = 0
        self.seconds = seconds
        self.t0 = None
        self.samples = []          # (elapsed, cumulative bytes)
        self._next = 0.0

    def write(self, b):
        now = time.perf_counter()
        if self.t0 is None:
            self.t0 = now
            self._next = SAMPLE_INTERVAL
        self.n += len(b)
        el = now - self.t0
        if el >= self._next:
            self._next += SAMPLE_INTERVAL
            self.samples.append((el, self.n))
        if el >= self.seconds:
            self.samples.append((el, self.n))
            raise StopTransfer
        return len(b)

    def writable(self):
        return True


def rate_between(samples, a, b):
    """MB/s between two samples, or None."""
    if a is None or b is None or b[0] <= a[0]:
        return None
    return (b[1] - a[1]) / (b[0] - a[0]) / 1048576.0


def main():
    ap = argparse.ArgumentParser(
        description="Measure sustained throughput through the clumsy path "
                    "(run on the subscriber).")
    ap.add_argument("--dsn", default=DEFAULT_PUB,
                    help="DSN of the PUBLISHER, from this machine")
    ap.add_argument("--seconds", type=float, default=30.0,
                    help="how long to stream. Must be long enough for TCP to "
                         "leave slow start - at 200 ms RTT that is tens of "
                         "seconds, not one.")
    ap.add_argument("--label", default="", help="what clumsy was set to")
    args = ap.parse_args()

    if not args.dsn:
        print("FAIL: no DSN. Pass --dsn or set R3_PUB_DSN to the PUBLISHER.")
        return 2
    too_short = args.seconds < 10
    if too_short:
        print("WARNING: fewer than 10 seconds cannot separate steady state "
              "from TCP slow start on a high-latency path. This result is an "
              "upper bound, not a measurement.")

    sql = (f"COPY (SELECT repeat('x', {ROW_BYTES}) "
           f"FROM generate_series(1, {MAX_ROWS})) TO STDOUT")
    sink = Sink(args.seconds)
    elapsed = 0.0

    try:
        c = psycopg2.connect(args.dsn, connect_timeout=10,
                             application_name="r3_throughput_probe")
        c.set_session(autocommit=True)
        k = c.cursor()
        t0 = time.perf_counter()
        try:
            k.copy_expert(sql, sink)
        except StopTransfer:
            pass
        elapsed = time.perf_counter() - t0
        try:
            c.close()          # the COPY was abandoned; just drop the socket
        except Exception:
            pass
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {str(exc)[:160]}")
        return 2

    if not sink.samples or sink.n == 0:
        print("FAIL: nothing transferred.")
        return 2

    mb = sink.n / 1048576.0
    overall = mb / elapsed if elapsed > 0 else 0.0

    # The tail, after the ramp.
    cut = sink.samples[-1][0] * RAMP_FRACTION
    tail = [s for s in sink.samples if s[0] >= cut]
    steady = rate_between(sink.samples, tail[0], tail[-1]) if len(tail) >= 2 else None
    ramp = rate_between(sink.samples, sink.samples[0],
                        tail[0]) if len(sink.samples) >= 2 else None

    # Per-interval rates over the tail, to see whether it is actually steady.
    inter = []
    for a, b in zip(tail, tail[1:]):
        r = rate_between(sink.samples, a, b)
        if r is not None:
            inter.append(r)

    print("=" * 66)
    print("SUSTAINED THROUGHPUT FROM THE PUBLISHER"
          + (f"   [{args.label}]" if args.label else ""))
    print("=" * 66)
    print(f"  streamed for       {elapsed:8.1f} s")
    print(f"  transferred        {mb:8.1f} MB")
    print(f"  overall            {overall:8.2f} MB/s   (includes the ramp)")
    if ramp is not None:
        print(f"  first half         {ramp:8.2f} MB/s")
    if steady is not None:
        print(f"  SECOND HALF        {steady:8.2f} MB/s   <- use this one")
    if len(inter) >= 3:
        print(f"  tail spread        {min(inter):8.2f} - {max(inter):.2f} MB/s"
              f"   (sd {st.pstdev(inter):.2f})")
    print()

    rc = 1 if too_short else 0
    if steady is None:
        print("  Not enough samples to separate steady state from the ramp.")
        print("  Increase --seconds.")
        return 1
    if ramp is not None and steady > 0 and ramp / steady > 1.5:
        print("  WARNING: the first half was much faster than the second. The")
        print("  path is still degrading - run longer before trusting this.")
        rc = 1
    if len(inter) >= 3 and steady > 0 and st.pstdev(inter) / steady > 0.5:
        print("  WARNING: the tail is not steady. Treat this as an upper bound.")
        rc = 1

    if steady >= 6.0:
        print(f"  This path can carry 6 MB/s.")
    else:
        print(f"  This path CANNOT carry 6 MB/s. A level run at 6 MB/s here")
        print(f"  would measure bandwidth starvation, not latency.")
        print(f"  About {steady*0.6:.2f} MB/s would leave 40% headroom.")
    print()
    print("  Set family F's byte rate below the LOWEST second-half figure")
    print("  across all the latency levels, or the levels are not comparable.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
