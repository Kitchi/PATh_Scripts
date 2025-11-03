#!/usr/bin/env python3
"""
Parse HTCondor job history JSON files into pandas DataFrame and save as parquet.

This script extracts job timing information from HTCondor history and saves it
in parquet format for later analysis and plotting.

Usage:
    python condor_to_parquet.py <json_file> [--output OUTPUT_PARQUET]

Example:
    python condor_to_parquet.py condor_history_944143.json --output jobs.parquet
"""

import json
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np


def parse_condor_history(json_file):
    """
    Parse HTCondor history JSON file and extract job timing data.

    Args:
        json_file: Path to HTCondor history JSON file

    Returns:
        pandas.DataFrame with job timing information
    """
    print(f"Parsing {json_file}...")

    with open(json_file, 'r') as f:
        data = json.load(f)

    records = []

    for job in data:
        # Basic job identifiers
        proc_id = job.get('ProcId')
        cluster_id = job.get('ClusterId')
        job_status = job.get('JobStatus', 0)
        exit_code = job.get('ExitCode', 0)

        # Extract timestamps (Unix timestamps)
        job_start_timestamp = job.get('JobCurrentStartDate')
        input_start_timestamp = job.get('JobCurrentStartTransferInputDate')
        input_end_timestamp = job.get('JobCurrentFinishTransferInputDate')
        output_start_timestamp = job.get('JobCurrentStartTransferOutputDate')
        output_end_timestamp = job.get('JobCurrentFinishTransferOutputDate')
        job_end_timestamp = job.get('JobFinishedHookTime')
        completion_timestamp = job.get('CompletionDate')

        # Convert timestamps to datetime objects
        job_start_time = datetime.fromtimestamp(job_start_timestamp) if job_start_timestamp else None
        input_start_time = datetime.fromtimestamp(input_start_timestamp) if input_start_timestamp else None
        input_end_time = datetime.fromtimestamp(input_end_timestamp) if input_end_timestamp else None
        output_start_time = datetime.fromtimestamp(output_start_timestamp) if output_start_timestamp else None
        output_end_time = datetime.fromtimestamp(output_end_timestamp) if output_end_timestamp else None
        job_end_time = datetime.fromtimestamp(job_end_timestamp) if job_end_timestamp else None
        completion_time = datetime.fromtimestamp(completion_timestamp) if completion_timestamp else None

        # Calculate durations
        input_transfer_duration = None
        if input_start_timestamp and input_end_timestamp:
            input_transfer_duration = input_end_timestamp - input_start_timestamp

        job_duration = None
        if job_start_timestamp and job_end_timestamp:
            job_duration = job_end_timestamp - job_start_timestamp

        output_transfer_duration = None
        if output_start_timestamp and output_end_timestamp:
            output_transfer_duration = output_end_timestamp - output_start_timestamp

        total_duration = None
        if job_start_timestamp and completion_timestamp:
            total_duration = completion_timestamp - job_start_timestamp

        # Determine if job failed
        failed = (job_status != 4) or (exit_code != 0)

        # Create record (all timestamps as datetime objects)
        record = {
            'cluster_id': cluster_id,
            'proc_id': proc_id,
            'job_status': job_status,
            'exit_code': exit_code,
            'failed': failed,

            # All timestamps as datetime objects
            'job_start_time': job_start_time,
            'input_start_time': input_start_time,
            'input_end_time': input_end_time,
            'output_start_time': output_start_time,
            'output_end_time': output_end_time,
            'job_end_time': job_end_time,
            'completion_time': completion_time,

            # Calculated durations (seconds)
            'input_transfer_duration': input_transfer_duration,
            'job_duration': job_duration,
            'output_transfer_duration': output_transfer_duration,
            'total_duration': total_duration,
        }

        records.append(record)

    df = pd.DataFrame(records)

    print(f"Parsed {len(df)} jobs")

    return df




def print_statistics(df):
    """Print summary statistics about the parsed data."""
    total_jobs = len(df)
    failed_jobs = df['failed'].sum()
    jobs_with_completion = df['completion_time'].notna().sum()
    jobs_without_completion = df['completion_time'].isna().sum()

    print("\n" + "=" * 60)
    print("Job Statistics")
    print("=" * 60)
    print(f"Total jobs: {total_jobs}")
    print(f"Failed jobs: {failed_jobs}")
    print(f"Success rate: {(total_jobs - failed_jobs) / total_jobs * 100:.1f}%")
    print()
    print(f"Jobs with CompletionDate: {jobs_with_completion} ({jobs_with_completion/total_jobs*100:.1f}%)")
    print(f"Jobs without CompletionDate: {jobs_without_completion} ({jobs_without_completion/total_jobs*100:.1f}%)")

    # Analyze jobs without completion time
    if jobs_without_completion > 0:
        incomplete_df = df[df['completion_time'].isna()]
        print("\nJobs without CompletionDate breakdown by JobStatus:")
        status_counts = incomplete_df['job_status'].value_counts().sort_index()
        for status, count in status_counts.items():
            pct = count / jobs_without_completion * 100
            status_name = {
                1: "Idle",
                2: "Running",
                3: "Removed",
                4: "Completed",
                5: "Held",
                6: "Transferring Output",
                7: "Suspended"
            }.get(status, f"Unknown ({status})")
            print(f"  Status {status} ({status_name}): {count} ({pct:.1f}%)")
    print()

    # Duration statistics (in seconds)
    print("Duration Statistics (seconds):")
    duration_cols = [
        'input_transfer_duration',
        'job_duration',
        'output_transfer_duration',
        'total_duration'
    ]

    for col in duration_cols:
        if col in df.columns:
            valid_data = df[col].dropna()
            if len(valid_data) > 0:
                print(f"\n{col}:")
                print(f"  Mean: {valid_data.mean():.2f} sec ({valid_data.mean()/60:.2f} min)")
                print(f"  Median: {valid_data.median():.2f} sec ({valid_data.median()/60:.2f} min)")
                print(f"  Min: {valid_data.min():.2f} sec")
                print(f"  Max: {valid_data.max():.2f} sec ({valid_data.max()/60:.2f} min)")
                print(f"  Std Dev: {valid_data.std():.2f} sec")

    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Parse HTCondor job history JSON files into pandas DataFrame and save as parquet'
    )
    parser.add_argument(
        'json_file',
        type=str,
        help='Path to HTCondor history JSON file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='condor_jobs.parquet',
        help='Output parquet file path (default: condor_jobs.parquet)'
    )

    args = parser.parse_args()

    # Validate input file
    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"Error: File not found: {args.json_file}")
        return 1

    # Check if parquet file already exists
    parquet_path = Path(args.output)
    loaded_from_existing = False

    if parquet_path.exists():
        print(f"Parquet file already exists: {args.output}")
        print(f"Loading existing parquet file instead of re-converting JSON...")
        df = pd.read_parquet(args.output)
        print(f"Loaded {len(df)} jobs from existing parquet file")
        loaded_from_existing = True
    else:
        # Parse job history
        df = parse_condor_history(args.json_file)

    if len(df) == 0:
        print("Error: No job data found in JSON file")
        return 1

    # Print statistics
    print_statistics(df)

    # Save to parquet (only if we parsed from JSON)
    if not loaded_from_existing:
        print(f"Saving DataFrame to {args.output}...")
        df.to_parquet(args.output, engine='pyarrow', compression='snappy')

        file_size = Path(args.output).stat().st_size / (1024 * 1024)  # Convert to MB
        print(f"DataFrame saved successfully ({file_size:.2f} MB)")
    else:
        file_size = Path(args.output).stat().st_size / (1024 * 1024)  # Convert to MB
        print(f"Using existing parquet file ({file_size:.2f} MB)")

    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    print(f"\nTo load the data later:")
    print(f"  import pandas as pd")
    print(f"  df = pd.read_parquet('{args.output}')")
    print(f"\nTo convert datetime to astropy.Time:")
    print(f"  from astropy.time import Time")
    print(f"  df['job_start_astropy_time'] = df['job_start_time'].apply(lambda x: Time(x, format='datetime') if pd.notna(x) else None)")

    return 0


if __name__ == '__main__':
    exit(main())
