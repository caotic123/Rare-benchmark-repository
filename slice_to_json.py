#!/usr/bin/env python3
import os
import re
import sys
import json

STEP_NAME_RE = re.compile(r"\(step\s+([^\s\)]+)")

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    root_abs = os.path.abspath(root)
    root_name = os.path.basename(root_abs.rstrip(os.sep))
    out_path = sys.argv[2] if len(sys.argv) > 2 else f"{root_name}.commands.json"

    results = []

    for dirpath, _, filenames in os.walk(root_abs):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                    for lineno, line in enumerate(fh, start=1):
                        # Only inspect lines that mention TRUST_THEORY_REWRITE
                        if "TRUST_THEORY_REWRITE" in line:
                            m = STEP_NAME_RE.search(line)
                            if m:
                                results.append({
                                    "folder": root_name,
                                    "file": os.path.relpath(fpath, root_abs),
                                    "command": m.group(1),
                                    "line": lineno
                                })
            except Exception:
                # Skip unreadable/binary files silently
                continue

    with open(out_path, "w", encoding="utf-8") as out:
        json.dump(results, out, ensure_ascii=False, indent=2)

    print(f"Wrote {len(results)} matches to {out_path}")

if __name__ == "__main__":
    main()
