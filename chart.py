#!/usr/bin/env python3
"""
proof_slice_report.py

Generates:
  - CSV summary per group (default: by 'root')
  - Outcomes bar chart (stacked or grouped)
  - Time-per-entries (inverse CDF) line plot
  - NEW: Scatter plot of entries (sample up to 1000), labeled by file name

Usage
-----
pip install pandas matplotlib
python proof_slice_report.py -i results.json -o report/
cat results.json | python proof_slice_report.py -i - -o report/

Key options
-----------
--group-by        Comma-separated fields to group by (default: root)
--chart           stacked|grouped (default: grouped)
--scatter-sample  Max entries to label in scatter (default: 1000)
--label-field     Field to extract filename labels from (default: smt2)
--img-fmt         png|svg|pdf (default: png)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ----------------------------- I/O ------------------------------------ #

def load_records(path: str) -> List[Dict[str, Any]]:
    if path == "-":
        return json.loads(sys.stdin.read())
    with open(path, "r", encoding="utf-8") as f:
        if path.lower().endswith(".jsonl"):
            return [json.loads(line) for line in f if line.strip()]
        return json.load(f)

_elapsed_re = re.compile(r"^\s*(\d+(?:\.\d+)?)(ns|ms|s|m)\s*$", re.IGNORECASE)

def parse_elapsed_to_seconds(val: Any) -> float:
    if val is None:
        return float("nan")
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    m = _elapsed_re.match(s)
    if not m:
        return float("nan")
    num = float(m.group(1)); unit = m.group(2)
    if unit == "ns":
        return num / 1_000_000_000.0
    if unit == "ms":
        return num / 1_000.0
    if unit == "s":
        return num
    if unit == "m":
        return num * 60.0
    return float("nan")

def normalize_frame(records: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    defaults = {
        "root": "UNKNOWN",
        "ok": False,
        "success": 0,
        "failed": 0,
        "panicked": False,
        "timeout": False,
        "elapsed": np.nan,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    df["ok"] = df["ok"].astype(bool)
    df["timeout"] = df["timeout"].astype(bool)
    df["panicked"] = df["panicked"].astype(bool)
    df["elapsed_sec"] = df["elapsed"].apply(parse_elapsed_to_seconds)

    df["is_success"] = df["ok"]
    df["is_timeout"] = (~df["ok"]) & (df["timeout"])
    df["is_panicked"] = (~df["ok"]) & (df["panicked"])
    df["is_otherfail"] = (~df["ok"]) & (~df["timeout"]) & (~df["panicked"])
    return df

# ----------------------------- Summary -------------------------------- #

def summarize(df: pd.DataFrame, group_fields: Sequence[str]) -> pd.DataFrame:
    g = df.groupby([df[f].fillna("NA") for f in group_fields], dropna=False)
    summary = pd.DataFrame({
        "jobs": g.size(),
        "successes": g["is_success"].sum(),
        "timeouts": g["is_timeout"].sum(),
        "panicked": g["is_panicked"].sum(),
        "other_fail": g["is_otherfail"].sum(),
        "success_rate_%": (g["is_success"].mean() * 100.0),
        "p50_elapsed_s": g["elapsed_sec"].median(),
        "p95_elapsed_s": g["elapsed_sec"].quantile(0.95),
    }).reset_index()
    summary = summary.rename(columns={f: f for f in group_fields})
    summary = summary.sort_values(by=["jobs", "success_rate_%"], ascending=[False, False])
    for c in ["success_rate_%", "p50_elapsed_s", "p95_elapsed_s"]:
        summary[c] = summary[c].round(3)
    return summary

# ------------------------------ Charts --------------------------------- #

def _labels_from_row(row: pd.Series, pref: str) -> str:
    # Pick preferred field, else fallbacks
    candidates = [pref, "alethe", "slice", "out"]
    for c in candidates:
        if c in row and isinstance(row[c], str) and row[c].strip():
            base = os.path.basename(row[c])
            return base
    return "(unknown)"

def plot_outcomes_stacked(summary: pd.DataFrame,
                          group_fields: Sequence[str],
                          out_path: str,
                          img_fmt: str = "png") -> str:
    labcol = "__label__"
    s = summary.copy()
    if len(group_fields) == 1:
        s[labcol] = s[group_fields[0]].astype(str)
    else:
        s[labcol] = s[group_fields].astype(str).agg(" / ".join, axis=1)

    labels = s[labcol].tolist()
    x = np.arange(len(labels))
    width = 0.6

    c_success = s["successes"].to_numpy()
    c_timeout = s["timeouts"].to_numpy()
    c_panicked = s["panicked"].to_numpy()
    c_other = s["other_fail"].to_numpy()

    fig, ax = plt.subplots(figsize=(max(8, min(18, 1.2 * len(labels))), 6))
    ax.bar(x, c_success, width, label="Success")
    ax.bar(x, c_timeout, width, bottom=c_success, label="Timeout")
    ax.bar(x, c_panicked, width, bottom=c_success + c_timeout, label="Panicked")
    ax.bar(x, c_other, width, bottom=c_success + c_timeout + c_panicked, label="Other fail")

    ax.set_title("Outcomes by group (stacked)")
    ax.set_xlabel(" / ".join(group_fields))
    ax.set_ylabel("count")
    ax.set_xticks(x, labels)
    ax.legend()

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"proof_outcomes_stacked.{img_fmt}")
    fig.tight_layout()
    fig.savefig(out_file, dpi=160)
    plt.close(fig)
    return out_file

def plot_outcomes_grouped(summary: pd.DataFrame,
                          group_fields: Sequence[str],
                          out_path: str,
                          img_fmt: str = "png") -> str:
    labcol = "__label__"
    s = summary.copy()
    if len(group_fields) == 1:
        s[labcol] = s[group_fields[0]].astype(str)
    else:
        s[labcol] = s[group_fields].astype(str).agg(" / ".join, axis=1)

    labels = s[labcol].tolist()
    x = np.arange(len(labels))
    series = [
        ("successes", "Success"),
        ("timeouts", "Timeout"),
        ("panicked", "Panicked"),
        ("other_fail", "Other fail"),
    ]
    m = len(series)
    width = 0.8 / m

    fig, ax = plt.subplots(figsize=(max(8, min(18, 1.2 * len(labels))), 6))
    for i, (col, title) in enumerate(series):
        y = s[col].to_numpy()
        ax.bar(x + (i - (m-1)/2) * width, y, width, label=title)

    ax.set_title("Outcomes by group (grouped)")
    ax.set_xlabel(" / ".join(group_fields))
    ax.set_ylabel("count")
    ax.set_xticks(x, labels)
    ax.legend(ncols=min(4, m))

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"proof_outcomes_grouped.{img_fmt}")
    fig.tight_layout()
    fig.savefig(out_file, dpi=160)
    plt.close(fig)
    return out_file

def plot_time_per_entries(df: pd.DataFrame, out_path: str, img_fmt: str = "png") -> str:
    """Inverse-CDF / rank plot: y=time, x=number of entries."""
    times = df["elapsed_sec"].dropna().to_numpy()
    if times.size == 0:
        times = np.array([0.0])
    times.sort()
    x = np.arange(1, times.size + 1)  # 1..N

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x, times, linewidth=1.5)
    ax.set_title("Time per number of entries (sorted by elapsed)")
    ax.set_xlabel("number of entries")
    ax.set_ylabel("elapsed time (seconds)")

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"time_per_entries.{img_fmt}")
    fig.tight_layout()
    fig.savefig(out_file, dpi=160)
    plt.close(fig)
    return out_file

def plot_scatter_entries(df: pd.DataFrame,
                         out_path: str,
                         img_fmt: str = "png",
                         sample_size: int = 1000,
                         label_field: str = "smt2") -> str:
    """
    Scatter plot where each point is an entry (random sample up to sample_size),
    labeled with the file name only (basename). Coordinates:
      x = entry rank after sorting by elapsed_sec (fastest→slowest)
      y = elapsed_sec
    """
    d = df[["elapsed_sec", label_field, "alethe", "slice", "out"]].copy()
    d = d.dropna(subset=["elapsed_sec"])
    if d.empty:
        # Create an empty-looking plot to avoid errors
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.set_title("Entries scatter (no data)")
        ax.set_xlabel("entry rank (by elapsed)")
        ax.set_ylabel("elapsed time (seconds)")
        os.makedirs(out_path, exist_ok=True)
        out_file = os.path.join(out_path, f"entries_scatter.{img_fmt}")
        fig.tight_layout(); fig.savefig(out_file, dpi=160); plt.close(fig)
        return out_file

    # Random sample (reproducible)
    if sample_size and len(d) > sample_size:
        d = d.sample(n=sample_size, random_state=42)

    # Sort by time to define rank
    d = d.sort_values("elapsed_sec", kind="mergesort").reset_index(drop=True)
    d["rank"] = np.arange(1, len(d) + 1)
    d["label"] = d.apply(lambda r: _labels_from_row(r, label_field), axis=1)

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.scatter(d["rank"], d["elapsed_sec"], s=12, alpha=0.9)

    # Annotate every sampled point with its filename (basename)
    for _, r in d.iterrows():
        ax.annotate(r["label"], (r["rank"], r["elapsed_sec"]),
                    textcoords="offset points", xytext=(2, 2),
                    fontsize=7, ha="left", va="bottom")

    ax.set_title(f"Entries scatter (n={len(d)}) — labeled by file name")
    ax.set_xlabel("entry rank (by elapsed, fastest → slowest)")
    ax.set_ylabel("elapsed time (seconds)")

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"entries_scatter.{img_fmt}")
    fig.tight_layout()
    fig.savefig(out_file, dpi=160)
    plt.close(fig)
    return out_file

# ------------------------------ CLI ------------------------------------ #

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize proof slicing results and generate charts.")
    p.add_argument("-i", "--input", required=True, help="Path to JSON/JSONL; use '-' for stdin.")
    p.add_argument("-o", "--out-dir", default="report", help="Directory to write outputs. Default: ./report")
    p.add_argument("--group-by", default="root", help="Comma-separated field(s) to group by. Default: root")
    p.add_argument("--top-n", type=int, default=None, help="Limit to top N groups for charting.")
    p.add_argument("--img-fmt", choices=("png", "svg", "pdf"), default="png", help="Image format for charts.")
    p.add_argument("--chart", choices=("stacked", "grouped"), default="grouped",
                   help="Outcomes chart style. Default: grouped.")
    p.add_argument("--scatter-sample", type=int, default=1000,
                   help="Max number of entries to label in the scatter. Default: 1000")
    p.add_argument("--label-field", default="smt2",
                   help="Field for file labels (fallbacks: alethe, slice, out). Default: smt2")
    return p.parse_args(argv)

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    try:
        records = load_records(args.input)
    except Exception as e:
        print(f"[ERROR] Failed to load input: {e}", file=sys.stderr)
        return 2
    if not isinstance(records, list):
        print("[ERROR] Input must be a JSON list.", file=sys.stderr)
        return 2

    df = normalize_frame(records)

    group_fields = [s.strip() for s in args.group_by.split(",") if s.strip()] or ["root"]
    summary = summarize(df, group_fields)

    chart_summary = summary
    if isinstance(args.top_n, int) and args.top_n > 0 and len(summary) > args.top_n:
        chart_summary = summary.nlargest(args.top_n, "jobs")

    os.makedirs(args.out_dir, exist_ok=True)
    csv_path = os.path.join(args.out_dir, "proof_summary.csv")
    summary.to_csv(csv_path, index=False)

    # Outcomes chart
    if args.chart == "grouped":
        outcomes_path = plot_outcomes_grouped(chart_summary, group_fields, args.out_dir, img_fmt=args.img_fmt)
    else:
        outcomes_path = plot_outcomes_stacked(chart_summary, group_fields, args.out_dir, img_fmt=args.img_fmt)

    # Rank line plot
    rank_path = plot_time_per_entries(df, args.out_dir, img_fmt=args.img_fmt)

    # Scatter with labels (random sample up to N)
    scatter_path = plot_scatter_entries(df, args.out_dir, img_fmt=args.img_fmt,
                                        sample_size=args.scatter_sample,
                                        label_field=args.label_field)

    # Console summary
    total_jobs = int(summary["jobs"].sum())
    total_success = int(summary["successes"].sum())
    overall_sr = (total_success / total_jobs * 100.0) if total_jobs else 0.0
    p50 = float(df["elapsed_sec"].median()) if total_jobs else float("nan")
    p95 = float(df["elapsed_sec"].quantile(0.95)) if total_jobs else float("nan")

    print(f"[OK] Wrote: {csv_path}")
    print(f"[OK] Wrote: {outcomes_path}")
    print(f"[OK] Wrote: {rank_path}")
    print(f"[OK] Wrote: {scatter_path}")
    print(f"Jobs={total_jobs}  Successes={total_success}  SuccessRate={overall_sr:.2f}%  "
          f"p50={p50:.3f}s  p95={p95:.3f}s")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
