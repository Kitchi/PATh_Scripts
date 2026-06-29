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

- average runtime: mean of (end - start) per job
- cumulative runtime: last end - first start
"""
import os
import re
import sys
from datetime import datetime, timedelta

FMT = "%Y-%m-%d %H:%M:%S.%f"
DT = r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d+)"
PATTERN = re.compile(
    r"Task tclean complete\..*?Start time:\s*" + DT + r"\s+End time:\s*" + DT,
    re.IGNORECASE,
)


def build_times(log_dir):
    """Scan log_dir, write times.txt, return its path.

    Logs are streamed line-by-line and results written incrementally, so
    memory stays flat regardless of log size or job count.
    """
    out_path = os.path.join(log_dir, "times.txt")
    out_abs = os.path.abspath(out_path)
    count = 0
    with open(out_path, "w") as out:
        for name in sorted(os.listdir(log_dir)):
            path = os.path.join(log_dir, name)
            if not os.path.isfile(path) or os.path.abspath(path) == out_abs:
                continue
            if not name.endswith(".log"):
                continue
            try:
                with open(path, errors="replace") as f:
                    for line in f:
                        m = PATTERN.search(line)
                        if m:
                            out.write("\t".join(m.groups()) + "\n")
                            count += 1
            except OSError:
                continue
    print(f"wrote {count} jobs to {out_path}")
    return out_path


def parse(path):
    """Yield (start, end) per job. Streams; holds one line at a time."""
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            parts = line.split()
            if len(parts) < 4:
                continue  # skip blank/malformed lines
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
    target = sys.argv[1] if len(sys.argv) > 1 else "times.txt"
    path = build_times(target) if os.path.isdir(target) else target

    n = 0
    total = timedelta()
    first_start = None
    last_end = None
    for start, end in parse(path):
        n += 1
        total += end - start
        if first_start is None or start < first_start:
            first_start = start
        if last_end is None or end > last_end:
            last_end = end

    if n == 0:
        sys.exit("no valid jobs found")

    avg = total / n
    cumulative = last_end - first_start

    print(f"jobs:               {n}")
    print(f"average runtime:    {fmt(avg)}")
    print(f"cumulative runtime: {fmt(cumulative)}")


if __name__ == "__main__":
    main()
