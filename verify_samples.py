#!/usr/bin/env python3
import argparse
import json
import os
import random
import subprocess
import sys
import time


def human_elapsed(ns: int) -> str:
    if ns < 1_000_000:
        return f"{ns}ns"
    if ns < 1_000_000_000:
        return f"{ns/1_000_000:.0f}ms"
    if ns < 60_000_000_000:
        return f"{ns/1_000_000_000:.2f}s"
    return f"{ns/60_000_000_000:.2f}m"


def run_elaborate(slice_path: str, smt2_path: str, rare_file: str, timeout_sec: int):
    """
    Returns: (ok, success_count, failed_count, panicked, elapsed_ns, timed_out)
    """
    cmd = [
        "carcara", "elaborate",
        slice_path,
        smt2_path,
        "--rare-file", rare_file,
        "--hole-solver", "rare_rewrite",
        "--expand-let-bindings",
        "--allow-int-real-subtyping",
        "--pipeline", "hole", "local",
        "--parse-hole-args",
        "--no-print-with-sharing",
    ]

    start_ns = time.monotonic_ns()
    timed_out = False
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=(None if timeout_sec <= 0 else timeout_sec),
        )
        output = result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        timed_out = True
        output = ""
    end_ns = time.monotonic_ns()
    elapsed_ns = end_ns - start_ns

    ok = True
    success = output.count("Elaboration successed")
    failed = output.count("Check failed:")
    panicked = "panicked at" in output

    if timed_out or panicked or failed > 0:
        ok = False

    return ok, success, failed, panicked, elapsed_ns, timed_out


def main():
    ap = argparse.ArgumentParser(
        description="Verify random samples from results_merged.json produce the same results."
    )
    ap.add_argument("--results", default="results_merged.json", help="JSON results file (default: results_merged.json)")
    ap.add_argument("--rare-file", default="big.rare", help="Path to .rare file (default: big.rare)")
    ap.add_argument("--samples", "-n", type=int, default=7, help="Number of random samples (default: 7)")
    ap.add_argument("--timeout", type=int, default=200, help="Timeout in seconds for each elaboration (default: 200)")
    ap.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    ap.add_argument("--base-dir", default=".", help="Base directory for resolving paths (default: .)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    with open(args.results, "r", encoding="utf-8") as f:
        results = json.load(f)

    if len(results) < args.samples:
        print(f"Warning: only {len(results)} entries available, sampling all", file=sys.stderr)
        samples = results
    else:
        samples = random.sample(results, args.samples)

    print(f"Verifying {len(samples)} random samples...\n")

    passed = 0
    failed_list = []

    for i, entry in enumerate(samples, 1):
        slice_path = os.path.join(args.base_dir, entry["slice"])
        smt2_path = os.path.join(args.base_dir, entry["smt2"])
        expected_ok = entry["ok"]

        print(f"[{i}/{len(samples)}] {entry['slice']}")
        print(f"  Expected: ok={expected_ok}")

        if not os.path.isfile(slice_path):
            print(f"  ERROR: slice file not found: {slice_path}")
            failed_list.append({"entry": entry, "reason": "slice file not found"})
            continue

        if not os.path.isfile(smt2_path):
            print(f"  ERROR: smt2 file not found: {smt2_path}")
            failed_list.append({"entry": entry, "reason": "smt2 file not found"})
            continue

        ok, succ, fail, pan, elapsed_ns, timed_out = run_elaborate(
            slice_path=slice_path,
            smt2_path=smt2_path,
            rare_file=args.rare_file,
            timeout_sec=args.timeout,
        )

        print(f"  Actual:   ok={ok} (success={succ}, failed={fail}, panicked={pan}, timeout={timed_out})")
        print(f"  Elapsed:  {human_elapsed(elapsed_ns)}")

        if ok == expected_ok:
            print("  PASS")
            passed += 1
        else:
            print("  FAIL - result mismatch!")
            failed_list.append({
                "entry": entry,
                "reason": "result mismatch",
                "expected_ok": expected_ok,
                "actual_ok": ok,
            })
            return
        print()

    print("=" * 60)
    print(f"Results: {passed}/{len(samples)} passed")

    if failed_list:
        print(f"\nFailed samples ({len(failed_list)}):")
        for f in failed_list:
            print(f"  - {f['entry']['slice']}: {f['reason']}")
        sys.exit(1)
    else:
        print("\nAll samples verified successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
