#!/usr/bin/env python3
"""
Generate family F's matrix and campaign for ONE remote-subscriber region.
STANDALONE.  Writes two small JSON files. Run on the PUBLISHER.

Why family F is now one level per region
----------------------------------------
Latency is no longer injected. Each RTT point is a physically separate
subscriber in a different Azure region, so a "level" is a whole campaign
against a different --sub-dsn rather than a step inside one run. That removes
the clumsy gate entirely: each region's family is a single level, repeated,
and runs completely unattended.

The RTT is MEASURED, not chosen. Deploy the subscriber, run check_latency.py
from it against the publisher, and pass the median here. That number becomes
network_latency_ms and is what the paper reports.

  py make_region_family.py --region local --rtt 0.4 \
        --vm-size Standard_D8ads_v6 --apply-mbps 48.0 --disk-caching None
  py make_region_family.py --region eastus --rtt 26.8 \
        --vm-size Standard_D8nds_v6 --apply-mbps 44.1 --disk-caching ReadWrite
  py make_region_family.py --region northeurope --rtt 103.2 \
        --vm-size Standard_D8ds_v6 --apply-mbps 41.7 --disk-caching ReadWrite
  py make_region_family.py --region centralindia --rtt 218.0 \
        --vm-size Standard_D8ads_v6 --apply-mbps 43.3 --disk-caching ReadWrite

--vm-size is REQUIRED. The four subscribers are not all the same size
(Azure capacity forced that), so the size has to be recorded per region
rather than asserted once. hw_inventory.sh prints all four.

Then, with R3_SUB_DSN pointing at that region's subscriber:

  .\\start_campaign.ps1 -Stage region -Region eastus

Exit codes
----------
  0  written
  2  bad arguments
"""

import argparse
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))

# Identical to family F's design in every respect except where the subscriber
# physically is. 6 MB/s is far below both the publisher ceiling (23.4 MB/s at
# one row per commit) and the apply knee (~48 MB/s), so nothing saturates and
# RTT is the only thing that differs between regions.
LEVEL = {
    "clients": 16,
    "rows_per_commit": 1,
    "row_bytes": 1000,
    "target_wal_mbps": 6,
    "duration_sec": 360,
}

REPEATS = [1, 2, 3]
SEEDS = {"1": 1011, "2": 2027, "3": 3041}


def main():
    ap = argparse.ArgumentParser(
        description="Write matrix and campaign files for one region's subscriber.")
    ap.add_argument("--region", required=True,
                    help="short name, e.g. local, eastus, westeurope, southeastasia")
    ap.add_argument("--rtt", type=float, required=True,
                    help="MEASURED median RTT in ms from check_latency.py, "
                         "run on that subscriber against the publisher")
    ap.add_argument("--vm-size", required=True,
                    help="the subscriber's ACTUAL Azure VM size, e.g. "
                         "Standard_D8ads_v6. Run hw_inventory.sh to read it "
                         "off the machine. Required because the four "
                         "subscribers are NOT all the same size and the "
                         "manuscript has to report what was built.")
    ap.add_argument("--apply-mbps", type=float, default=None,
                    help="sustained apply throughput in MB/s that "
                         "preflight_region.py measured on THIS subscriber. "
                         "This is what makes the size difference defensible: "
                         "a machine with 4x headroom over the 6 MB/s offered "
                         "load is not the bottleneck.")
    ap.add_argument("--disk-caching", default=None,
                    help="host cache policy on the data disk (None, ReadOnly, "
                         "ReadWrite) as hw_inventory.sh reports it.")
    ap.add_argument("--duration", type=int, default=None,
                    help="override seconds per level. TESTING ONLY - the "
                         "paper's runs use the matrix default of "
                         f"{LEVEL['duration_sec']} s, and a shorter level "
                         "changes what the numbers mean.")
    ap.add_argument("--repeats", type=int, default=len(REPEATS))
    ap.add_argument("--out", default=HERE)
    args = ap.parse_args()

    if not re.fullmatch(r"[a-z0-9]{2,20}", args.region):
        print("FAIL: --region must be lower-case letters and digits, e.g. eastus")
        return 2
    if args.rtt < 0 or args.rtt > 1000:
        print("FAIL: --rtt must be a measured millisecond value")
        return 2

    level = dict(LEVEL)
    if args.duration and args.duration != LEVEL["duration_sec"]:
        level["duration_sec"] = args.duration
        print(f"  NOTE: duration overridden to {args.duration} s "
              f"(the paper's value is {LEVEL['duration_sec']} s). This file is "
              f"a TEST artefact, not a result.")

    fam = f"F_lat_{args.region}"
    matrix_name = f"matrix_{fam}.json"
    camp_name = f"campaign_{fam}.json"

    matrix = {
        "family": fam,
        "description": (
            f"Family F, {args.region}. Replication lag against REAL network "
            f"round-trip time: the subscriber is a physically separate machine "
            f"in the {args.region} Azure region, so the {args.rtt:.1f} ms is "
            f"genuine inter-region latency, not emulation. Nothing is injected "
            f"and nothing needs calibrating. The offered load is "
            f"{level['target_wal_mbps']} MB/s, identical in every region. "
            f"That it sits far below saturation is established per region by "
            f"preflight_region.py, which measures this subscriber's sustained "
            f"apply throughput before the campaign and requires at least four "
            f"times the offered load - see the hardware block. RTT is the only "
            f"quantity that differs between the points of this "
            f"dose-response."),
        "superseded_capacity_figures": {
            "note": ("Earlier drafts of this description cited an apply knee "
                     "of ~48 MB/s and a publisher ceiling of 23.4 MB/s from "
                     "the family A calibration. The apply-knee figure is "
                     "WITHDRAWN: every family A-E run was executed against a "
                     "subscriber running stock PostgreSQL defaults - "
                     "shared_buffers 128MB against the specified 8GB, "
                     "checkpoint_timeout 5min against 15min, track_io_timing "
                     "off - which was discovered and corrected before family "
                     "F ran. Each run's manifest records the subscriber "
                     "settings it actually ran under, so this is checkable "
                     "rather than asserted."),
            "publisher_ceiling_mbps": 23.4,
            "publisher_ceiling_status": (
                "retained. It is a commit-rate bound measured on the "
                "publisher (15,844 commits/s at one row per commit) and does "
                "not depend on the subscriber's configuration."),
            "apply_knee_mbps_withdrawn": 48.0,
            "replaced_by": ("measured_apply_mbps in the hardware block of "
                            "this file, measured on THIS subscriber in its "
                            "corrected configuration"),
        },
        "rtt_provenance": {
            "measured_median_ms": args.rtt,
            "how": ("check_latency.py --measure-only --samples 200, run ON THIS "
                    "SUBSCRIBER against the publisher's private address over the "
                    "peered VNet. Median of 200 application-level round trips on "
                    "one already-established connection, so TCP and TLS setup are "
                    "not counted."),
            "note": ("This replaces clumsy/WinDivert injection, which was "
                     "measured on this testbed to have a ~31 ms quantisation "
                     "floor and a throughput ceiling that varied eightfold "
                     "across repeats of one condition. See "
                     "clumsy_calibration.json - it is reported as a Methods "
                     "limitation, not used as an instrument."),
        },
        # Held identical in every region. If any of these differs between
        # points, the comparison is not of distance alone. The subscription is
        # named mysub on EVERY subscriber because monitor.py, orchestrate.py,
        # supervisor.py and verify_setup.py all match on
        # application_name='mysub' and subname='mysub'; a per-region name makes
        # the harness blind to the replication it is measuring.
        "held_constant": {
            "subscription_name": "mysub",
            "copy_data": False,
            "streaming": "off",
            "binary": False,
            "synchronous_commit": "off",
            "publisher": "unchanged, centralus",
            "active_slots_during_run": 1,
        },
        # NOT held constant, and said so plainly. The intention was one VM
        # size everywhere. Azure refused: Standard_D8nds_v6 is
        # Location-restricted in northeurope and centralindia, so the sizes
        # differ between regions. Recording the real size per region is the
        # only version of this a reviewer can check.
        #
        # The argument that the difference does not carry the result is not
        # "the processors are comparable". It is that each subscriber was
        # measured, before its campaign, to sustain several times the offered
        # load, so none of them was the limiting resource. apply_mbps below is
        # that measurement for this region.
        "hardware": {
            "vm_size": args.vm_size,
            "data_disk_caching": args.disk_caching,
            "measured_apply_mbps": args.apply_mbps,
            "offered_load_mbps": level["target_wal_mbps"],
            "headroom_x": (round(args.apply_mbps / level["target_wal_mbps"], 1)
                           if args.apply_mbps else None),
            "duration_sec": level["duration_sec"],
            "duration_is_paper_default":
                level["duration_sec"] == LEVEL["duration_sec"],
            "how_measured": ("preflight_region.py, run ON THIS SUBSCRIBER "
                             "before the campaign. It requires at least 4x "
                             "the offered load and refuses to give a GO "
                             "below that."),
            "note": ("VM sizes are not identical across regions - see "
                     "subscriber_hardware in the campaign record and the "
                     "Methods limitation. vCPU count (8) and memory (32 GiB) "
                     "ARE identical; the generation and processor family are "
                     "not. Every other software-side quantity in "
                     "held_constant above IS identical and was verified per "
                     "region by preflight_region.py."),
        },
        "levels": [dict(level,
                        level_id="F01",
                        label=f"{args.rtt:.1f} ms real RTT @ {level['target_wal_mbps']} MB/s ({args.region})",
                        network_latency_ms=round(args.rtt, 1))],
    }

    reps = list(range(1, max(1, args.repeats) + 1))
    campaign = {
        "name": f"round3_{fam}",
        "description": (
            f"Family F against the {args.region} subscriber. Fully UNATTENDED - "
            f"there is nothing to set between levels because the latency is the "
            f"network's own. Point R3_SUB_DSN at this region's subscriber and "
            f"make sure every OTHER subscription is disabled first, or the "
            f"publisher is feeding more than one standby and the offered load "
            f"is not what it says."),
        "families": [{
            "family": fam,
            "config": matrix_name,
            "repeats": reps,
            "seeds": {str(r): SEEDS.get(str(r), 1000 + r * 7) for r in reps},
            "no_randomise": True,
            "max_run_seconds": 5400,
        }],
    }

    for name, obj in ((matrix_name, matrix), (camp_name, campaign)):
        path = os.path.join(args.out, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2)
        print(f"  wrote {name}")

    mins = (level["duration_sec"] + 90) * len(reps) / 60.0
    print()
    print(f"  family      {fam}")
    print(f"  RTT         {args.rtt:.1f} ms (measured)")
    print(f"  duration    {level['duration_sec']} s per level"
          + ("" if level['duration_sec'] == LEVEL['duration_sec']
             else "   <- OVERRIDDEN, this is a test artefact"))
    print(f"  hardware    {args.vm_size}"
          + (f", caching {args.disk_caching}" if args.disk_caching else "")
          + (f", apply {args.apply_mbps:.1f} MB/s "
             f"({args.apply_mbps / LEVEL['target_wal_mbps']:.1f}x the offered load)"
             if args.apply_mbps else ", apply throughput NOT recorded"))
    print(f"  repeats     {len(reps)}   -> about {mins:.0f} minutes, unattended")
    print()
    print("  Before starting, ON THE PUBLISHER, confirm only this subscriber is")
    print("  attached - a second active slot means a second standby is being fed:")
    print("     SELECT slot_name, active FROM pg_replication_slots;")
    print()
    print(f"  Then:   .\\start_campaign.ps1 -Stage region -Region {args.region}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
