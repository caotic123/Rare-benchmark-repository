#!/usr/bin/env python3
"""
Regression test: Re-run elaboration on previously successful/failed cases to verify
that recent modifications haven't broken anything or have fixed issues.
"""
import argparse
import json
import os
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

def run_elaborate(slice_path: str, smt2_path: str, rare_file: str, timeout_sec: int = 200):
    """Run carcara elaborate and return (ok, success, failed, panicked, timed_out, elapsed_ns, output)"""
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
    output = ""
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

    success = output.count("Elaboration successed")
    failed = output.count("Check failed:")
    panicked = "panicked at" in output
    ok = not (timed_out or panicked or failed > 0) and success > 0

    return ok, success, failed, panicked, timed_out, elapsed_ns, output

def main():
    ap = argparse.ArgumentParser(description="Regression test for carcara elaboration")
    ap.add_argument("--results", default="results.json", help="Path to results.json")
    ap.add_argument("--rare-file", default="big.rare", help="Path to .rare file")
    ap.add_argument("--count", type=int, default=200, help="Number of cases to test")
    ap.add_argument("--timeout", type=int, default=200, help="Timeout in seconds per test")
    ap.add_argument("--base-dir", default="/home/caotic/Workspace/Benchmarks", help="Base directory for paths")
    ap.add_argument("--mode", choices=["success", "failed"], default="success",
                    help="Test previously successful or failed cases")
    ap.add_argument("--output", default=None, help="Output JSON file for results")
    args = ap.parse_args()

    # Load results
    results_path = args.results if os.path.isabs(args.results) else os.path.join(args.base_dir, args.results)
    with open(results_path, "r") as f:
        results = json.load(f)

    # Filter cases based on mode
    candidates = []
    seen = set()
    for r in results:
        key = r.get("slice")
        if not key or key in seen:
            continue

        if args.mode == "success":
            if r.get("ok") and not r.get("panicked") and not r.get("timeout"):
                seen.add(key)
                candidates.append(r)
        else:  # failed mode - exclude timeouts
            if not r.get("ok") and not r.get("timeout"):
                seen.add(key)
                candidates.append(r)

    mode_desc = "successful" if args.mode == "success" else "failed (non-timeout)"
    print(f"Found {len(candidates)} unique {mode_desc} cases in results.json")
    test_count = min(args.count, len(candidates))
    print(f"Testing {test_count} cases...")
    print("-" * 60)

    passed = 0
    now_passing = []
    still_failing = []
    output_results = []

    for i, case in enumerate(candidates[:args.count]):
        slice_path = os.path.join(args.base_dir, case["slice"])
        smt2_path = os.path.join(args.base_dir, case["smt2"])

        if not os.path.exists(slice_path):
            print(f"[{i+1}/{test_count}] SKIP (missing slice): {case['slice']}")
            continue
        if not os.path.exists(smt2_path):
            print(f"[{i+1}/{test_count}] SKIP (missing smt2): {case['smt2']}")
            continue

        ok, succ, fail, pan, timeout, elapsed_ns, output = run_elaborate(
            slice_path, smt2_path, args.rare_file, args.timeout
        )

        elapsed = human_elapsed(elapsed_ns)

        result_entry = {
            "root": case.get("root"),
            "alethe": case.get("alethe"),
            "hole": case.get("hole"),
            "line": case.get("line"),
            "slice": case.get("slice"),
            "smt2": case.get("smt2"),
            "ok": ok,
            "success": succ,
            "failed": fail,
            "panicked": pan,
            "timeout": timeout,
            "elapsed": elapsed,
            "previous_ok": case.get("ok", False),
        }
        output_results.append(result_entry)

        if ok:
            passed += 1
            if args.mode == "failed":
                status = "NOW PASS"
                now_passing.append(result_entry)
            else:
                status = "PASS"
            print(f"[{i+1}/{test_count}] {status} ({elapsed}): {case['hole']} in {os.path.basename(case['alethe'])}")
        else:
            reason = []
            if timeout:
                reason.append("timeout")
            if pan:
                reason.append("panic")
            if fail > 0:
                reason.append(f"{fail} failed")
            if succ == 0:
                reason.append("no success")
            reason_str = ", ".join(reason) if reason else "unknown"

            if args.mode == "failed":
                status = "STILL FAIL"
                still_failing.append({**result_entry, "reason": reason_str})
            else:
                status = "FAIL"
            print(f"[{i+1}/{test_count}] {status} ({reason_str}, {elapsed}): {case['hole']} in {os.path.basename(case['alethe'])}")

    print("-" * 60)
    print(f"Results: {passed}/{test_count} passed ({100*passed/test_count:.1f}%)")

    if args.mode == "failed":
        print(f"\nNow passing: {len(now_passing)}")
        print(f"Still failing: {len(still_failing)}")

    # Save output if requested
    if args.output:
        output_path = args.output if os.path.isabs(args.output) else os.path.join(args.base_dir, args.output)
        with open(output_path, "w") as f:
            json.dump(output_results, f, indent=2)
        print(f"\nResults saved to: {output_path}")

    sys.exit(0 if passed == test_count else 1)

if __name__ == "__main__":
    main()
