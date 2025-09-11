#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import shutil
import subprocess
from typing import Dict, Any, Generator

def human_elapsed(ns: int) -> str:
    if ns < 1_000_000:
        return f"{ns}ns"
    if ns < 1_000_000_000:
        return f"{ns/1_000_000:.0f}ms"
    if ns < 60_000_000_000:
        return f"{ns/1_000_000_000:.2f}s"
    return f"{ns/60_000_000_000:.2f}m"

def iter_jobs(fobj) -> Generator[Dict[str, Any], None, None]:
    data = fobj.read()
    data_stripped = data.strip()
    if not data_stripped:
        return
    # Try full JSON first
    try:
        obj = json.loads(data_stripped)
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield item
        elif isinstance(obj, dict):
            yield obj
        else:
            raise ValueError("Top-level JSON must be object or array of objects")
        return
    except json.JSONDecodeError:
        pass
    # Fallback: JSON Lines
    for i, line in enumerate(data.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(json.dumps({"status":"error","reason":"invalid JSONL line","line_no":i,"error":str(e)}))
            continue
        if isinstance(obj, dict):
            yield obj
        else:
            print(json.dumps({"status":"error","reason":"JSONL item not an object","line_no":i}))

def find_alethe(search_root: str, rel_or_name: str) -> str:
    cand = os.path.join(search_root, rel_or_name)
    if os.path.isfile(cand):
        return cand
    # fallback by basename search
    base = os.path.basename(rel_or_name)
    for dirpath, _dirnames, filenames in os.walk(search_root):
        if base in filenames:
            return os.path.join(dirpath, base)
    return ""

def main():
    ap = argparse.ArgumentParser(
        description="Run carcara slice from JSON job specs (assumes CWD == job.folder)"
    )
    ap.add_argument("input", nargs="?", default="-",
                    help="JSON file (object, array) or JSONL. Default: stdin")
    ap.add_argument("--out-root", default=None,
                    help="Directory where 'sliced_proofs' will be created. "
                         "Default: parent of CWD. Example: --out-root .. or --out-root /repo")
    ap.add_argument("--debug", action="store_true",
                    help="Print debug info and resolved paths")
    args = ap.parse_args()

    # Check carcara availability
    if shutil.which("carcara") is None:
        print(json.dumps({"status":"error","reason":"carcara not found in PATH"}))
        sys.exit(1)

    cwd = os.getcwd()
    parent = os.path.abspath(os.path.join(cwd, ".."))
    out_root_parent = os.path.abspath(args.out_root) if args.out_root else parent
    sliced_root = os.path.join(out_root_parent, "sliced_proofs")
    os.makedirs(sliced_root, exist_ok=True)

    if args.debug:
        print(json.dumps({
            "debug":"paths",
            "cwd": cwd,
            "out_root_parent": out_root_parent,
            "sliced_root": sliced_root
        }))

    search_root = cwd  # Only search within current folder

    if args.input == "-" or args.input == "/dev/stdin":
        fobj = sys.stdin
    else:
        fobj = open(args.input, "r", encoding="utf-8")

    with fobj:
        for job in iter_jobs(fobj):
            folder = job.get("folder", "") or ""
            file_field = job.get("file")
            command = job.get("command")
            line_no = job.get("line", None)

            if not file_field or not command:
                print(json.dumps({
                    "status":"error",
                    "reason":"missing file or command",
                    "job": job
                }))
                continue

            # Locate input files relative to CWD
            src_alethe = find_alethe(search_root, file_field)
            if not src_alethe:
                print(json.dumps({
                    "status":"not_found",
                    "reason":"alethe file not found under CWD",
                    "cwd": cwd,
                    "file": file_field
                }))
                continue

            if not src_alethe.endswith(".alethe"):
                print(json.dumps({
                    "status":"error",
                    "reason":"located file does not end with .alethe",
                    "path": os.path.relpath(src_alethe, cwd)
                }))
                continue

            src_smt2 = src_alethe[:-7]  # strip ".alethe"
            if not os.path.isfile(src_smt2):
                print(json.dumps({
                    "status":"error",
                    "reason":"matching .smt2 not found",
                    "alethe": os.path.relpath(src_alethe, cwd)
                }))
                continue
            
            if '.' in command:
                continue

            rel_from_search = os.path.relpath(src_alethe, search_root)
            rel_dir = os.path.dirname(rel_from_search)
            base_name = os.path.splitext(os.path.basename(src_smt2))[0]

            # Use the folder reported in JSON for naming under sliced_proofs;
            # if absent, fall back to the name of the current directory.
            top_folder_name = folder if folder else os.path.basename(cwd)

            out_dir = os.path.join(sliced_root, top_folder_name, rel_dir, base_name)
            os.makedirs(out_dir, exist_ok=True)

            out_file = os.path.join(out_dir, f"{base_name}__from-{command}.smt2.alethe")
            err_file = out_file + ".stderr"

            if args.debug:
                print(json.dumps({
                    "debug":"resolved_paths",
                    "src_alethe": os.path.relpath(src_alethe, cwd),
                    "src_smt2": os.path.relpath(src_smt2, cwd),
                    "out_file": os.path.relpath(out_file, out_root_parent),
                    "err_file": os.path.relpath(err_file, out_root_parent)
                }))

            start_ns = time.monotonic_ns()
            with open(out_file, "w", encoding="utf-8") as out_f, open(err_file, "w", encoding="utf-8") as err_f:
                proc = subprocess.check_output(
                    ["carcara", "slice", "--from", str(command), src_alethe, src_smt2, "--no-print-with-sharing"],
                    text=True,
                )
                out_f.write(proc)
                
            end_ns = time.monotonic_ns()
            elapsed = human_elapsed(end_ns - start_ns)

            status = "ok" if proc.__len__() != 0 else "error"
            # Keep .stderr even if empty when debug=false? If empty and not debug, remove.
            if not args.debug:
                try:
                    if os.path.getsize(err_file) == 0:
                        os.remove(err_file)
                except FileNotFoundError:
                    pass

            result = {
                "folder": top_folder_name,
                "file": file_field,
                "command": command,
                "line": line_no,
                "out": os.path.relpath(out_file, out_root_parent),
                "status": status,
                "elapsed": elapsed,
            }

            if status != "ok":
                # Attach short hint to stderr path
                result["stderr"] = os.path.relpath(err_file, out_root_parent)

            print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
