#!/usr/bin/env python3
"""
Validate the metadata-only analyzer against exact, row-level ground truth.

Run make_test_table.py first. This compares, for every subject in both test
tables:

    metadata estimate (min/max bound overlap, no rows read)
        vs.
    exact answer (every data file opened and scanned)

The estimate must never UNDER-count -- an under-count would mean the planner
misses a file containing the subject's data, which for an erasure argument is
a correctness bug, not an approximation. Over-counting is expected and is the
honest conservative direction.

Usage:
    python3 validate.py --warehouse /abs/path/to/_wh
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scatter import candidates_for_key, collect_files, probe_points  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    args = ap.parse_args()

    wh = Path(args.warehouse).resolve()
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "test",
        **{"uri": f"sqlite:///{wh}/catalog.db", "warehouse": f"file://{wh}"},
    )
    truth = json.loads((wh / "ground_truth.json").read_text())

    ok = True
    print("=" * 74)
    print("VALIDATION: metadata-only estimate vs. exact row-level ground truth")
    print("=" * 74)

    for name in ("events_scattered", "events_clustered"):
        table = catalog.load_table(f"lake.{name}")
        files = collect_files(table, "user_id")
        data_files = files["data_files"]
        gt = truth[name]["per_user_file_count"]

        exact: list[int] = []
        est: list[int] = []
        undercounts = 0
        for user, true_n in gt.items():
            e, _ = candidates_for_key(data_files, int(user))
            exact.append(true_n)
            est.append(e)
            if e < true_n:
                undercounts += 1

        errs = [e - t for e, t in zip(est, exact)]
        med_exact = statistics.median(exact)
        med_est = statistics.median(est)

        print(f"\n{name}  ({len(data_files)} data files, {len(gt)} subjects)")
        print(f"  exact files touched      p50 = {med_exact:.0f}")
        print(f"  estimated files touched  p50 = {med_est:.0f}")
        print(f"  over-count (est - exact) p50 = {statistics.median(errs):.0f}  "
              f"max = {max(errs)}")
        print(f"  UNDER-counts (must be 0) ... {undercounts}")
        if undercounts:
            ok = False
            print("  *** FAIL: estimate missed files that truly contain the subject")
        else:
            print("  PASS: estimate is a valid upper bound for every subject")

        # No-key probe estimate: what you get with zero subject knowledge.
        probes = probe_points(data_files, 200)
        probe_est = [candidates_for_key(data_files, p)[0] for p in probes]
        if probe_est:
            print(f"  no-key probe estimate    p50 = {statistics.median(probe_est):.0f}  "
                  f"(vs exact {med_exact:.0f}) -- usable without any subject keys")

    print("\n" + "-" * 74)
    print("Expected contrast: the scattered table should show ~every file touched")
    print("per subject; the clustered table should show ~1. If both look the same,")
    print("the bounds are not being read correctly.")
    print("-" * 74)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
