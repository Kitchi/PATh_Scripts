#!/usr/bin/env python3
"""Compute average and cumulative runtime of tclean jobs.

Usage:
    runtime_stats.py <times.txt>   # read an existing times file
    runtime_stats.py <log_dir>     # build times.txt from logs, then report

times.txt format (one job per line):
    <start_date> <start_time> <end_date> <end_time>
    date = YYYY-MM-DD, time = HH:MM:SS.ffffff

When given a directory, every file is scanned for lines like
    Task tclean complete. Start time: <date> <time> End time: <date> <time>
(equivalent to the original grep|awk pipeline), and times.txt is written
into that directory before stats are computed.

Output:
    - mean runtime +/- std
    - median runtime +/- std via MAD
    - cumulative runtime: last end - first start
"""
import argparse
import concurrent.futures
import os
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd

FMT = "%Y-%m-%d %H:%M:%S.%f"
DT = r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d+)"
PATTERN = re.compile(
    r"Task tclean complete\..*?Start time:\s*" + DT + r"\s+End time:\s*" + DT,
    re.IGNORECASE,
)


def _scan_file(path):
    """Return list of (start_str, end_str) tuples found in one file."""
    results = []
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m = PATTERN.search(line)
                if m:
                    results.append(m.groups())
    except OSError:
        pass
    return results


def build_times(log_dir, workers=8):
    """Scan log_dir multithreaded, write times.txt, return its path."""
    out_path = os.path.join(log_dir, "times.txt")
    out_abs = os.path.abspath(out_path)

    log_files = []
    for name in sorted(os.listdir(log_dir)):
        path = os.path.join(log_dir, name)
        if (
            not os.path.isfile(path)
            or os.path.abspath(path) == out_abs
            or not name.endswith(".casa")
        ):
            continue
        log_files.append(path)

    all_records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exc:
        futs = [exc.submit(_scan_file, p) for p in log_files]
        for fut in concurrent.futures.as_completed(futs):
            all_records.extend(fut.result())

    all_records.sort()
    with open(out_path, "w") as out:
        for groups in all_records:
            out.write("\t".join(groups) + "\n")

    print(f"wrote {len(all_records)} jobs to {out_path}")
    return out_path


def parse(path):
    """Yield (start, end) per job."""
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            parts = line.split()
            if len(parts) < 4:
                continue
            try:
                start = datetime.strptime(f"{parts[0]} {parts[1]}", FMT)
                end = datetime.strptime(f"{parts[2]} {parts[3]}", FMT)
            except ValueError:
                sys.stderr.write(f"skipping line {lineno}: {line!r}")
                continue
            yield start, end


def fmt(td):
    total = td.total_seconds()
    h, rem = divmod(int(total), 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}h {m:02d}m {s:02d}s ({total:.3f}s)"


def main():
    parser = argparse.ArgumentParser(description="Compute average and cumulative runtime of tclean jobs.")
    parser.add_argument(
        "target", nargs="?", default="times.txt",
        help="Path to a times.txt file or a directory of .casa logs (default: times.txt)",
    )
    parser.add_argument(
        "-j", "--jobs", type=int, default=8,
        help="Number of parallel threads for scanning log files (default: 8)",
    )
    args = parser.parse_args()

    target = args.target
    if os.path.isdir(target):
        path = build_times(target, workers=args.jobs)
    elif os.path.isfile(target):
        path = target
    else:
        sys.exit(f"error: '{target}' not found.\nProvide a directory of .casa logs or an existing times.txt file.")

    starts, ends = [], []
    for start, end in parse(path):
        starts.append(start)
        ends.append(end)

    if not starts:
        sys.exit("no valid jobs found")

    df = pd.DataFrame({"start": pd.to_datetime(starts), "end": pd.to_datetime(ends)})
    df["duration"] = (df["end"] - df["start"]).dt.total_seconds()

    durations = df["duration"].values
    mean = np.mean(durations)
    std = np.std(durations, ddof=1)
    median = np.median(durations)
    mad = np.median(np.abs(durations - median))

    cumulative = (df["end"].max() - df["start"].min()).total_seconds()

    print(f"jobs:               {len(df)}")
    print(f"mean runtime:       {fmt(pd.Timedelta(seconds=mean))}  +/- {std:.3f}s")
    print(f"median runtime:     {fmt(pd.Timedelta(seconds=median))}  +/- {mad:.3f}s")
    print(f"cumulative runtime: {fmt(pd.Timedelta(seconds=cumulative))}")


if __name__ == "__main__":
    main()