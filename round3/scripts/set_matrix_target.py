#!/usr/bin/env python3
"""
Round 3 - change a family's target WAL rate, with the reason recorded.

    py set_matrix_target.py --matrix matrix_E_duration.json --target 44 \
        --reason "just below the re-measured apply knee of 52 MB/s"

    py set_matrix_target.py --matrix matrix_B_concurrency.json \
        --levels B07,B08,B09,B10 --target 52 \
        --reason "the near-knee block, re-set from the corrected calibration"

Without --apply it shows the change and writes nothing.

WHY NOT JUST EDIT THE JSON
--------------------------
Two of the target rates in families B and E were chosen relative to the
subscriber's apply knee, and that knee was measured on a subscriber running
stock defaults. When the knee is re-measured the targets have to move with it.

Hand-editing works until it doesn't: the rate appears in target_wal_mbps AND
in each level's human-readable label, and a file where the label says
"40 MB/s" while the field says 52 is worse than either, because the run report
prints the label. This changes both, and appends a retarget_history entry
saying what the value was, what it became, and why - so the matrix carries its
own provenance rather than relying on someone remembering.

It refuses to touch duration, clients, rows_per_commit or row_bytes. Those are
the independent variables of their families; a script that could quietly move
them is a script that can invalidate an experiment.

EXIT CODES
----------
  0  done, or shown with no --apply
  1  nothing matched
  2  bad arguments
"""

import argparse
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(
        description="Set target_wal_mbps on a matrix file, with provenance.")
    ap.add_argument("--matrix", required=True,
                    help="matrix file name or path, e.g. matrix_E_duration.json")
    ap.add_argument("--target", type=float, required=True,
                    help="the new target WAL rate in MB/s")
    ap.add_argument("--levels", default=None,
                    help="comma-separated level ids to change. Default: every "
                         "level whose current target equals the most common "
                         "one in the file, so a two-block family like B is "
                         "not flattened by accident.")
    ap.add_argument("--reason", required=True,
                    help="one sentence, recorded in the file. Required.")
    ap.add_argument("--apply", action="store_true",
                    help="write the change. Without it, nothing is written.")
    a = ap.parse_args()

    path = a.matrix if os.path.isabs(a.matrix) else os.path.join(HERE, a.matrix)
    if not os.path.isfile(path):
        print(f"no such file: {path}")
        return 2
    if a.target <= 0 or a.target > 200:
        print(f"--target {a.target} is not a plausible MB/s rate")
        return 2

    with open(path, encoding="utf-8") as fh:
        m = json.load(fh)
    levels = m.get("levels") or []
    if not levels:
        print(f"{os.path.basename(path)} has no levels")
        return 1

    if a.levels:
        want = {s.strip() for s in a.levels.split(",") if s.strip()}
        unknown = want - {l.get("level_id") for l in levels}
        if unknown:
            print(f"no such level(s): {', '.join(sorted(unknown))}")
            print(f"levels in this file: "
                  f"{', '.join(l.get('level_id', '?') for l in levels)}")
            return 2
        chosen = [l for l in levels if l.get("level_id") in want]
    else:
        # A family can have more than one block at different rates - family B
        # has a sub-saturation block at 8 MB/s and a near-knee block at 40.
        # Defaulting to "every level" would flatten both to one rate and
        # destroy the design. Default to the single most common rate instead,
        # and say which.
        counts = {}
        for l in levels:
            counts[l.get("target_wal_mbps")] = \
                counts.get(l.get("target_wal_mbps"), 0) + 1
        if len(counts) > 1:
            common = max(counts, key=lambda k: counts[k])
            print(f"\n  This file has {len(counts)} distinct target rates: "
                  f"{', '.join(f'{k} MB/s x{v}' for k, v in sorted(counts.items(), key=lambda kv: -kv[1]))}")
            print(f"  Defaulting to the {common} MB/s block only "
                  f"({counts[common]} level(s)).")
            print(f"  Pass --levels to choose explicitly.\n")
            chosen = [l for l in levels
                      if l.get("target_wal_mbps") == common]
        else:
            chosen = list(levels)

    print(f"{os.path.basename(path)}   family {m.get('family', '?')}")
    print(f"  {'level':<8}{'was':>8}{'becomes':>10}   label")
    print("  " + "-" * 74)
    changes = []
    for l in chosen:
        old = l.get("target_wal_mbps")
        if old == a.target:
            continue
        newlabel = _relabel(l.get("label", ""), old, a.target)
        changes.append((l, old, newlabel))
        print(f"  {l.get('level_id', '?'):<8}{str(old):>8}{a.target:>10.0f}   "
              f"{newlabel[:56]}")

    if not changes:
        print(f"\n  Every chosen level is already at {a.target:.0f} MB/s. "
              f"Nothing to do.\n")
        return 0

    if not a.apply:
        print(f"\n  {len(changes)} level(s) would change. Nothing was written.")
        print(f"  Add --apply to write it.\n")
        return 0

    for l, old, newlabel in changes:
        l["target_wal_mbps"] = (int(a.target) if float(a.target).is_integer()
                                else a.target)
        if newlabel:
            l["label"] = newlabel

    m.setdefault("retarget_history", []).append({
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "reason": a.reason,
        "new_target_wal_mbps": a.target,
        "levels": [{"level_id": l.get("level_id"),
                    "was_target_wal_mbps": old} for l, old, _n in changes],
        "tool": "set_matrix_target.py",
    })

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(m, fh, indent=2)
    print(f"\n  Wrote {os.path.basename(path)} - {len(changes)} level(s) "
          f"changed, reason recorded in retarget_history.\n")
    return 0


def _relabel(label, old, new):
    """Keep the human-readable label honest about the rate it now runs at."""
    if not label or old is None:
        return label
    def fmt(v):
        return f"{int(v)}" if float(v).is_integer() else f"{v}"
    pat = re.compile(rf"(?<![\d.]){re.escape(fmt(old))}\s*MB/s")
    if pat.search(label):
        return pat.sub(f"{fmt(new)} MB/s", label)
    return label


if __name__ == "__main__":
    sys.exit(main())
