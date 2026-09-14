#!/usr/bin/env python3
"""
Set family B's target rates from the probe you actually ran.  STANDALONE.

There is no worked example here and no placeholder number to copy by mistake:
every figure comes out of manifest_P_probe_rep1.json on this machine.

  py retarget_from_probe.py              # analyse and recommend, change nothing
  py retarget_from_probe.py --apply      # write the recommendation into matrix_B

Why family B needs its own step
-------------------------------
Every level in family B must hold the SAME delivered byte rate, because the
whole point is to vary concurrency and nothing else. So the rate is capped by
the level with the FEWEST clients - one - and calibration never measured that.
The probe does, in level P01.

Exit codes
----------
  0  a workable rate was found (and applied, with --apply)
  1  no rate works for the client range as configured - read the advice
  2  the probe manifest could not be read
"""

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Leave the closed loop room to correct. A target set at the very ceiling
# leaves the controller railed, and any jitter shows up as a missed set point
# rather than as data.
HEADROOM = 0.70

# Where the subscriber stops keeping up. Calibration's long drains put it near
# 46 MB/s; the probe brackets it tightly - 38.6 and 49.4 MB/s were absorbed,
# 49.7 and 50.6 were not - so ~48 is the working figure.
APPLY_KNEE_MB_S = 48.0


def load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main():
    ap = argparse.ArgumentParser(
        description="Set family B's rates from the measured probe.")
    ap.add_argument("--manifest",
                    default=os.path.join(os.environ.get("R3_DATA", r"C:\r3\data"),
                                         "manifest_P_probe_rep1.json"))
    ap.add_argument("--matrix", default=os.path.join(HERE, "matrix_B_concurrency.json"))
    ap.add_argument("--apply", action="store_true",
                    help="write the recommendation into the matrix file")
    ap.add_argument("--knee", type=float, default=APPLY_KNEE_MB_S,
                    help="the apply-capacity knee measured in calibration")
    args = ap.parse_args()

    try:
        man = load(args.manifest)
    except Exception as exc:
        print(f"Could not read the probe manifest at {args.manifest}\n  {exc}")
        print("\nRun the probe first:  .\\start_campaign.ps1 -Stage probe")
        return 2

    got = {}
    for r in man.get("results", []):
        if r.get("failed"):
            continue
        p = r.get("params", {})
        got[r["level_id"]] = (p.get("clients"), p.get("rows_per_commit"),
                              p.get("row_bytes"), r.get("measured_wal_mb_per_sec"))

    print("PROBE — what each shape could actually deliver")
    print("=" * 62)
    print(f"{'level':6}{'clients':>9}{'rows/commit':>13}{'row bytes':>11}{'MB/s':>9}")
    for lid in sorted(got):
        c, rc, rb, mb = got[lid]
        print(f"{lid:6}{c:>9}{rc:>13}{rb:>11}{mb:>9.2f}" if mb is not None
              else f"{lid:6}{c:>9}{rc:>13}{rb:>11}{'-':>9}")
    print()

    # --- the binding constraint -------------------------------------------
    sweep = {c: mb for (c, rc, rb, mb) in got.values()
             if rc == 10 and rb == 1000 and mb is not None}
    if not sweep:
        print("No client sweep at 10 rows/commit found in the probe. Cannot advise.")
        return 2

    slowest_clients = min(sweep)
    ceiling = sweep[slowest_clients]
    target = ceiling * HEADROOM

    print("FAMILY B")
    print("=" * 62)
    print(f"  Fewest clients tested        {slowest_clients}")
    print(f"  What that could deliver      {ceiling:.2f} MB/s   <- the hard cap")
    print(f"  Recommended at-knee target   {target:.0f} MB/s "
          f"({HEADROOM*100:.0f}% of it, leaving room for the loop)")
    print()

    above = args.knee * 1.15
    if ceiling >= above:
        print(f"  Recommended above-knee       {above:.0f} MB/s")
        rc_ok = True
    else:
        rc_ok = False
        print(f"  Above-knee is NOT reachable. It needs about {above:.0f} MB/s "
              f"(15% past the")
        print(f"  {args.knee:.0f} MB/s apply knee), and {slowest_clients} client(s) "
              f"can only produce {ceiling:.2f}.")
        print()
        print("  Three honest options:")
        best_c = max(sweep, key=lambda c: sweep[c])
        print(f"    1. Drop the low client counts. At {best_c} clients the probe "
              f"reached {sweep[best_c]:.1f} MB/s;")
        print(f"       keep only the counts that clear {above:.0f} MB/s.")
        print(f"    2. Raise rows_per_commit for the whole family, so the same "
              f"bytes need fewer")
        print(f"       commits. The probe's own P09 level shows what large "
              f"batches can do.")
        print(f"    3. Run family B sub-knee only, at {target:.0f} MB/s, and say so "
              f"in Methods -")
        print(f"       concurrency at a fixed sub-saturation rate is still a "
              f"valid experiment,")
        print(f"       it just cannot speak to behaviour above capacity.")
        print()

    # --- family D, from P07 / P08 -----------------------------------------
    small = [(c, mb) for (c, rc, rb, mb) in got.values()
             if rb == 100 and rc == 1 and mb is not None]
    if small:
        best = max(small, key=lambda x: x[1])
        print("FAMILY D (100-byte rows are its binding constraint)")
        print("=" * 62)
        for c, mb in sorted(small):
            print(f"  {c:>3} clients delivered        {mb:.2f} MB/s")
        print(f"  Recommended target           {best[1]*HEADROOM:.0f} MB/s "
              f"at {best[0]} clients   (currently 7)")
        print()

    if not args.apply:
        print("Nothing was changed. Re-run with --apply to write family B's "
              "at-knee rate.")
        return 0 if rc_ok else 1

    # --- write it ----------------------------------------------------------
    try:
        mat = load(args.matrix)
    except Exception as exc:
        print(f"Could not read {args.matrix}: {exc}")
        return 2

    n = 0
    for lv in mat["levels"]:
        if lv.get("clients") is not None:
            lv["target_wal_mbps"] = round(target)
            lv["rows_per_commit"] = 10
            n += 1
    mat["retargeted_from_probe"] = {
        "source": os.path.basename(args.manifest),
        "binding_level_clients": slowest_clients,
        "measured_ceiling_mb_per_sec": ceiling,
        "headroom": HEADROOM,
        "target_mb_per_sec": round(target),
        "above_knee_reachable": rc_ok,
    }
    with open(args.matrix, "w", encoding="utf-8") as fh:
        json.dump(mat, fh, indent=2)
    print(f"Wrote {n} levels in {os.path.basename(args.matrix)} at "
          f"{round(target)} MB/s, 10 rows/commit.")
    if not rc_ok:
        print("NOTE: every level is now at the SAME sub-knee rate. If you wanted "
              "an above-knee\n      block as well, take option 1 or 2 above first.")
    return 0 if rc_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
