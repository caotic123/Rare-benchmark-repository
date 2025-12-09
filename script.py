#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Iterable, List, Tuple, Optional

STEP_NAME_RE = re.compile(r"\(step\s+([^\s\)]+)")
HOLE_MARKER = "TRUST_THEORY_REWRITE"

def human_elapsed(ns: int) -> str:
    if ns < 1_000_000:
        return f"{ns}ns"
    if ns < 1_000_000_000:
        return f"{ns/1_000_000:.0f}ms"
    if ns < 60_000_000_000:
        return f"{ns/1_000_000_000:.2f}s"
    return f"{ns/60_000_000_000:.2f}m"

def read_file_lines(path: str) -> Iterable[Tuple[int, str]]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for i, line in enumerate(fh, start=1):
            yield i, line

def holes_in_alethe(path: str) -> List[Tuple[str, int]]:
    holes: List[Tuple[str, int]] = []
    for lineno, line in read_file_lines(path):
        if HOLE_MARKER in line:
            m = STEP_NAME_RE.search(line)
            if m:
                name = m.group(1)
                if "." in name:
                    continue
                holes.append((name, lineno))
    seen = set()
    uniq: List[Tuple[str, int]] = []
    for h in holes:
        if h[0] not in seen:
            seen.add(h[0])
            uniq.append(h)
    return uniq

def rel_to(root: str, path: str) -> str:
    try:
        return os.path.relpath(path, root)
    except Exception:
        return path

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def append_json_array(path: str, entry: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump([entry], f, ensure_ascii=False, indent=2)
            f.write("\n")
        return
    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        if f.tell() == 0:
            f.write(json.dumps([entry], ensure_ascii=False, indent=2).encode("utf-8"))
            f.write(b"\n")
            return
        step = 1024
        size = f.tell()
        pos = max(0, size - step)
        f.seek(pos)
        chunk = f.read().decode("utf-8", errors="ignore")
        while "]" not in chunk and pos > 0:
            pos = max(0, pos - step)
            f.seek(pos)
            chunk = f.read(min(step, pos + step)).decode("utf-8", errors="ignore") + chunk
        last_bracket = chunk.rfind("]")
        if last_bracket == -1:
            f.seek(0)
            try:
                existing = json.load(f) or []
            except Exception:
                existing = []
            existing.append(entry)
            f.seek(0)
            f.truncate()
            json.dump(existing, f, ensure_ascii=False, indent=2)
            f.write("\n".encode("utf-8"))
            return
        abs_pos = pos + last_bracket
        f.seek(abs_pos)
        tail = ",\n" + json.dumps(entry, ensure_ascii=False, indent=2) + "\n]"
        f.write(tail.encode("utf-8"))

def find_smt2_beside(alethe: str) -> Optional[str]:
    if alethe.endswith(".smt2.alethe"):
        cand = alethe[:-7]
        return cand if os.path.isfile(cand) else None
    if alethe.endswith(".alethe"):
        base, _ = os.path.splitext(alethe)
        cand = base
        return cand if os.path.isfile(cand) else None
    return None

def find_existing_smt2_for_base(out_dir: str, base_name: str) -> Optional[str]:
    cand = os.path.join(out_dir, f"{base_name}.smt2")
    return cand if os.path.isfile(cand) else None

def search_smt2_in_tree(root: str, base_name: str) -> Optional[str]:
    """Search for base_name.smt2 anywhere under root directory."""
    target = f"{base_name}.smt2"
    for dirpath, _, filenames in os.walk(root):
        if target in filenames:
            return os.path.join(dirpath, target)
    return None

def restore_smt2_from_git(smt2_path: str, repo_root: str, debug: bool = False) -> bool:
    """Try to restore a deleted .smt2 file from git history."""
    try:
        # Get relative path from repo root
        rel_path = os.path.relpath(smt2_path, repo_root)

        # Find the last commit that had this file
        result = subprocess.run(
            ["git", "rev-list", "-n", "1", "HEAD", "--", rel_path],
            capture_output=True, text=True, cwd=repo_root
        )
        if result.returncode != 0 or not result.stdout.strip():
            # Try with full history
            result = subprocess.run(
                ["git", "log", "--all", "--format=%H", "-n", "1", "--", rel_path],
                capture_output=True, text=True, cwd=repo_root
            )
            if result.returncode != 0 or not result.stdout.strip():
                return False

        commit = result.stdout.strip()

        # Get the file content from that commit
        result = subprocess.run(
            ["git", "show", f"{commit}:{rel_path}"],
            capture_output=True, text=True, cwd=repo_root
        )
        if result.returncode == 0 and result.stdout:
            os.makedirs(os.path.dirname(smt2_path), exist_ok=True)
            with open(smt2_path, "w", encoding="utf-8") as f:
                f.write(result.stdout)
            if debug:
                print(json.dumps({"debug": "smt2_restored_from_git", "path": smt2_path, "commit": commit[:8]}))
            return True
    except Exception as e:
        if debug:
            print(json.dumps({"debug": "git_restore_error", "path": smt2_path, "error": str(e)}))
    return False

def run_slice(from_hole: str, alethe_path: str, smt2_path: str, out_slice_path: str,
              parse_hole_args: bool, no_print_with_sharing: bool, debug: bool) -> Tuple[bool, Optional[str]]:
    ensure_dir(os.path.dirname(out_slice_path))
    cmd = [
        "carcara", "slice",
        "--from", str(from_hole),
        alethe_path,
        smt2_path,
    ]
    if parse_hole_args:
        cmd.append("--parse-hole-args")
    if no_print_with_sharing:
        cmd.append("--no-print-with-sharing")
    if debug:
        print(json.dumps({"debug": "slice_cmd", "cmd": cmd, "out": out_slice_path}))
    stderr_path = out_slice_path + ".stderr"
    with open(out_slice_path, "w", encoding="utf-8") as f_out, open(stderr_path, "w", encoding="utf-8") as f_err:
        proc = subprocess.run(cmd, stdout=f_out, stderr=f_err, text=True)
    if os.path.getsize(out_slice_path) == 0 or proc.returncode != 0:
        return False, stderr_path
    try:
        if os.path.getsize(stderr_path) == 0 and not debug:
            os.remove(stderr_path)
    except FileNotFoundError:
        pass
    return True, None

def run_elaborate(slice_path: str, smt2_path: str, out_log_path: str, rare_file: str,
                  allow_int_real: bool, add_pipeline: bool, no_print_with_sharing: bool,
                  parse_hole_args: bool, timeout_sec: int) -> Tuple[bool, int, int, bool, int, bool]:
    """
    Returns: (ok, success_count, failed_count, panicked, elapsed_ns, timed_out)
    """
    ensure_dir(os.path.dirname(out_log_path))
    cmd = [
        "carcara", "elaborate",
        slice_path,
        smt2_path,
        "--rare-file", rare_file,
        "--hole-solver", "rare_rewrite",
        "--expand-let-bindings",
    ]
    if allow_int_real:
        cmd.append("--allow-int-real-subtyping")
    if add_pipeline:
        cmd.extend(["--pipeline", "hole", "local"])
    if parse_hole_args:
        cmd.append("--parse-hole-args")
    if no_print_with_sharing:
        cmd.append("--no-print-with-sharing")

    start_ns = time.monotonic_ns()
    timed_out = False
    with open(out_log_path, "w", encoding="utf-8") as f_log:
        try:
            subprocess.run(
                cmd,
                stdout=f_log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=(None if timeout_sec is None or timeout_sec <= 0 else timeout_sec),
            )
        except subprocess.TimeoutExpired:
            timed_out = True
            # Leave a clear marker in the log
            try:
                f_log.write(f"\n[timeout] Elaboration exceeded {timeout_sec}s and was terminated.\n")
            except Exception:
                pass
    end_ns = time.monotonic_ns()
    elapsed_ns = end_ns - start_ns

    # Parse the log
    ok = True
    success = 0
    failed = 0
    panicked = False
    try:
        with open(out_log_path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
        success = data.count("Elaboration successed")
        failed  = data.count("Check failed:")
        if "panicked at" in data:
            panicked = True
    except Exception:
        ok = False

    if timed_out or panicked or failed > 0:
        ok = False

    return ok, success, failed, panicked, elapsed_ns, timed_out

def main():
    ap = argparse.ArgumentParser(
        description="Incrementally slice 'Theory rewrite' holes and elaborate them with Carcara."
    )
    ap.add_argument("root", nargs="?", default=".", help="Root folder to scan for *.smt2.alethe files (default: .)")
    ap.add_argument("--out-root", default=None, help="Base dir to write 'sliced_proofs'. Default: alongside ROOT (parent of ROOT).")
    ap.add_argument("--rare-file", default="big.rare", help="Path to .rare file (default: big.rare)")
    ap.add_argument("--results", default="results.json", help="JSON array file to append per-slice elaboration results (default: results.json)")
    ap.add_argument("--elab-timeout-sec", type=int, default=200, help="Timeout in seconds for each elaboration (0 disables). Default: 60")
    ap.add_argument("--debug", action="store_true", help="Verbose debug output")
    ap.add_argument("--no-cache", action="store_true", help="Ignore cached slices and elaboration outputs, re-run everything from scratch")
    ap.add_argument("--pre-slice", action="store_true", help="Run slice_to_json.py + slice_from_json.py first to pre-generate all slices before elaboration")
    ap.add_argument("--no-move", action="store_true", help="Do not move the original .smt2 (copy instead).")
    # Defaults reflect your benchmark flags
    ap.add_argument("--allow-int-real-subtyping", action="store_true", default=True)
    ap.add_argument("--pipeline-hole-local", action="store_true", default=True)
    ap.add_argument("--parse-hole-args", action="store_true", default=True)
    ap.add_argument("--no-print-with-sharing", action="store_true", default=True)
    args = ap.parse_args()

    if shutil.which("carcara") is None:
        print("error: 'carcara' not found in PATH", file=sys.stderr)
        sys.exit(1)

    root_abs = os.path.abspath(args.root)
    root_name = os.path.basename(root_abs.rstrip(os.sep))
    out_root_parent = os.path.abspath(args.out_root) if args.out_root else os.path.abspath(os.path.join(root_abs, ".."))
    sliced_root = os.path.join(out_root_parent, "sliced_proofs")
    ensure_dir(sliced_root)

    if args.debug:
        print(json.dumps({"debug": "paths", "root": root_abs, "sliced_root": sliced_root}))

    # Pre-slice step: run slice_to_json.py + slice_from_json.py if requested and commands.json doesn't exist
    if args.pre_slice:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        slice_to_json = os.path.join(script_dir, "slice_to_json.py")
        slice_from_json = os.path.join(script_dir, "slice_from_json.py")
        commands_json = os.path.join(out_root_parent, f"{root_name}.commands.json")

        # Only run if commands.json doesn't exist (skip if cached)
        if os.path.isfile(commands_json) and not args.no_cache:
            print(json.dumps({"status": "pre_slice", "step": "skipped", "reason": "commands.json already exists", "path": commands_json}))
        else:
            if not os.path.isfile(slice_to_json) or not os.path.isfile(slice_from_json):
                print(json.dumps({"status": "error", "reason": "slice_to_json.py or slice_from_json.py not found", "script_dir": script_dir}))
                sys.exit(1)

            # Generate commands JSON
            print(json.dumps({"status": "pre_slice", "step": "slice_to_json", "root": root_abs, "output": commands_json}))
            try:
                subprocess.run([sys.executable, slice_to_json, root_abs, commands_json], check=True)
            except subprocess.CalledProcessError as e:
                print(json.dumps({"status": "error", "reason": "slice_to_json.py failed", "returncode": e.returncode}))
                sys.exit(1)

            if not os.path.isfile(commands_json):
                print(json.dumps({"status": "warning", "reason": "no commands.json generated, no holes found"}))
            else:
                # Run slice_from_json to generate slices
                print(json.dumps({"status": "pre_slice", "step": "slice_from_json", "input": commands_json}))
                try:
                    slice_from_args = [sys.executable, slice_from_json, commands_json, "--out-root", out_root_parent]
                    if args.debug:
                        slice_from_args.append("--debug")
                    # Run from root_abs so paths resolve correctly
                    subprocess.run(slice_from_args, check=True, cwd=root_abs)
                except subprocess.CalledProcessError as e:
                    print(json.dumps({"status": "error", "reason": "slice_from_json.py failed", "returncode": e.returncode}))
                    sys.exit(1)

            print(json.dumps({"status": "pre_slice", "step": "done"}))

    # Track already-processed alethe files to avoid duplicates
    processed_alethe = set()

    for dirpath, dirnames, filenames in os.walk(root_abs):
        # Skip sliced_proofs directory to avoid processing generated slices
        if "sliced_proofs" in dirnames:
            dirnames.remove("sliced_proofs")

        for fname in filenames:
            if not fname.endswith(".smt2.alethe"):
                continue
            # Skip sliced alethe files (they have __from- in the name)
            if "__from-" in fname:
                continue

            alethe_path = os.path.join(dirpath, fname)

            # Skip if already processed (avoid duplicates)
            if alethe_path in processed_alethe:
                continue
            processed_alethe.add(alethe_path)

            rel_dir = os.path.relpath(dirpath, root_abs)
            base_name = os.path.splitext(os.path.basename(alethe_path[:-7]))[0]

            out_dir = os.path.join(sliced_root, root_name, rel_dir, base_name)
            ensure_dir(out_dir)

            smt2_src = find_smt2_beside(alethe_path)
            smt2_dest = find_existing_smt2_for_base(out_dir, base_name)

            if smt2_dest:
                smt2_for_slice = smt2_dest
            elif smt2_src and os.path.isfile(smt2_src):
                smt2_for_slice = smt2_src
            else:
                guessed = os.path.join(dirpath, f"{base_name}.smt2")
                if os.path.isfile(guessed):
                    smt2_for_slice = guessed
                else:
                    # Fallback: search entire root and sliced_proofs for the smt2 file
                    found = search_smt2_in_tree(root_abs, base_name)
                    if not found:
                        found = search_smt2_in_tree(sliced_root, base_name)
                    if found:
                        smt2_for_slice = found
                        if args.debug:
                            print(json.dumps({"debug": "smt2_found_via_search", "path": found}))
                    else:
                        # Try to restore from git history
                        expected_smt2 = os.path.join(dirpath, f"{base_name}.smt2")
                        if restore_smt2_from_git(expected_smt2, root_abs, args.debug):
                            smt2_for_slice = expected_smt2
                            print(json.dumps({"status": "restored", "reason": "smt2 restored from git", "path": rel_to(root_abs, expected_smt2)}))
                        else:
                            print(json.dumps({
                                "status":"not_found",
                                "reason":"matching .smt2 not found (also not in git)",
                                "alethe": rel_to(root_abs, alethe_path)
                            }))
                            continue

            holes = holes_in_alethe(alethe_path)
            if args.debug:
                print(json.dumps({"debug":"holes", "alethe": rel_to(root_abs, alethe_path), "count": len(holes)}))

            for hole_name, line_no in holes:
                slice_name = f"{base_name}__from-{hole_name}.smt2.alethe"
                slice_path = os.path.join(out_dir, slice_name)
                out_log_path = os.path.join(out_dir, f"{base_name}__from-{hole_name}.out")

                slice_cached = os.path.exists(slice_path) and not args.no_cache
                if not slice_cached:
                    ok_slice, stderr_path = run_slice(
                        from_hole=hole_name,
                        alethe_path=alethe_path,
                        smt2_path=smt2_for_slice,
                        out_slice_path=slice_path,
                        parse_hole_args=args.parse_hole_args,
                        no_print_with_sharing=args.no_print_with_sharing,
                        debug=args.debug
                    )
                    if not ok_slice:
                        print(json.dumps({
                            "status":"slice_error",
                            "alethe": rel_to(root_abs, alethe_path),
                            "hole": hole_name,
                            "line": line_no,
                            "stderr": rel_to(out_root_parent, stderr_path) if stderr_path else None
                        }, ensure_ascii=False))
                        continue

                smt2_dest = find_existing_smt2_for_base(out_dir, base_name)
                if not smt2_dest:
                    if not smt2_src or not os.path.isfile(smt2_src):
                        if os.path.isfile(smt2_for_slice):
                            smt2_src = smt2_for_slice
                    target = os.path.join(out_dir, f"{base_name}.smt2")
                    if smt2_src and os.path.isfile(smt2_src):
                        if args.no_move:
                            shutil.copy2(smt2_src, target)
                            moved_action = "copied"
                        else:
                            try:
                                shutil.move(smt2_src, target)
                                moved_action = "moved"
                            except shutil.Error:
                                shutil.copy2(smt2_src, target)
                                try:
                                    os.remove(smt2_src)
                                except OSError:
                                    pass
                                moved_action = "moved_copy"
                        if args.debug:
                            print(json.dumps({"debug":"smt2_transfer", "action": moved_action, "to": rel_to(out_root_parent, target)}))
                        smt2_dest = target
                    else:
                        smt2_dest = smt2_for_slice

                elaborate_cached = os.path.exists(out_log_path) and not args.no_cache
                if elaborate_cached:
                    print(json.dumps({
                        "status": "ok",
                        "cached": True,
                        "root": root_name,
                        "alethe": rel_to(root_abs, alethe_path),
                        "hole": hole_name,
                        "slice": rel_to(out_root_parent, slice_path),
                        "out": rel_to(out_root_parent, out_log_path)
                    }, ensure_ascii=False))
                    continue

                ok, succ, fail, pan, elapsed_ns, timed_out = run_elaborate(
                    slice_path=slice_path,
                    smt2_path=smt2_dest,
                    out_log_path=out_log_path,
                    rare_file=args.rare_file,
                    allow_int_real=args.allow_int_real_subtyping,
                    add_pipeline=args.pipeline_hole_local,
                    no_print_with_sharing=args.no_print_with_sharing,
                    parse_hole_args=args.parse_hole_args,
                    timeout_sec=args.elab_timeout_sec,
                )

                result = {
                    "root": root_name,
                    "alethe": rel_to(root_abs, alethe_path),
                    "hole": hole_name,
                    "line": line_no,
                    "slice": rel_to(out_root_parent, slice_path),
                    "smt2": rel_to(out_root_parent, smt2_dest) if smt2_dest else None,
                    "out": rel_to(out_root_parent, out_log_path),
                    "ok": ok,
                    "success": succ,
                    "failed": fail,
                    "panicked": pan,
                    "timeout": timed_out,
                    "elapsed": human_elapsed(elapsed_ns),
                }

                print(json.dumps(result, ensure_ascii=False))
                append_json_array(os.path.join(out_root_parent, args.results), result)

if __name__ == "__main__":
    main()
