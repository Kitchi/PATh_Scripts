#!/usr/bin/env python3
"""
Plot maximum concurrent jobs as a function of time.

This script creates a plot showing how many jobs were running concurrently
over time, binned at a specified time resolution (default: 30 seconds).

Usage:
    python plot_concurrent_jobs.py <parquet_file> [--output OUTPUT_PDF] [--resolution SECONDS]

Examples:
    # Default 30-second resolution
    python plot_concurrent_jobs.py condor_jobs.parquet

    # 1-minute resolution
    python plot_concurrent_jobs.py condor_jobs.parquet --resolution 60

    # Custom output file
    python plot_concurrent_jobs.py condor_jobs.parquet --output concurrent.pdf
"""

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta


def plot_concurrent_jobs(df, output_file='concurrent_jobs.pdf', resolution_seconds=30, show_plot=False):
    """
    Plot the number of concurrent jobs over time.

    Args:
        df: pandas.DataFrame with job timing columns
        output_file: Path to output PDF file
        resolution_seconds: Time bin size in seconds
    """
    print(f"Creating concurrent jobs plot (resolution: {resolution_seconds}s)...")

    # Filter jobs with valid start and completion times
    df_valid = df[df['job_start_time'].notna() & df['completion_time'].notna()].copy()

    print(f"Valid jobs with timing data: {len(df_valid)}/{len(df)}")

    if len(df_valid) == 0:
        print("Error: No valid jobs with timing data")
        return

    # Find overall time range
    start_time = df_valid['job_start_time'].min()
    end_time = df_valid['completion_time'].max()
    total_duration = (end_time - start_time).total_seconds()

    print(f"Time range: {start_time} to {end_time}")
    print(f"Total duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")

    # Create time bins
    num_bins = int(np.ceil(total_duration / resolution_seconds))
    time_bins = pd.date_range(start=start_time, periods=num_bins+1,
                               freq=f'{resolution_seconds}s')

    print(f"Creating {num_bins} time bins of {resolution_seconds}s each...")

    # Count concurrent jobs in each bin
    concurrent_counts = []
    bin_centers = []
    bin_centers_hours = []

    for i in range(len(time_bins) - 1):
        bin_start = time_bins[i]
        bin_end = time_bins[i + 1]
        bin_center = bin_start + (bin_end - bin_start) / 2

        # Calculate relative time in hours from start
        relative_hours = (bin_center - start_time).total_seconds() / 3600

        # Count jobs that were running during this bin
        # A job is running if: job_start_time <= bin_center < completion_time
        running = df_valid[
            (df_valid['job_start_time'] <= bin_center) &
            (df_valid['completion_time'] > bin_center)
        ]

        concurrent_counts.append(len(running))
        bin_centers.append(bin_center)
        bin_centers_hours.append(relative_hours)

    # Calculate statistics
    max_concurrent = max(concurrent_counts)
    mean_concurrent = np.mean(concurrent_counts)
    median_concurrent = np.median(concurrent_counts)

    print(f"\nConcurrency Statistics:")
    print(f"  Maximum concurrent jobs: {max_concurrent}")
    print(f"  Mean concurrent jobs: {mean_concurrent:.1f}")
    print(f"  Median concurrent jobs: {median_concurrent:.1f}")

    # Create the plot
    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot concurrent jobs over time (using relative hours)
    ax.plot(bin_centers_hours, concurrent_counts, linewidth=1.5, color='#2ca02c',
            label='Concurrent Jobs')

    # Add horizontal line for maximum
    ax.axhline(max_concurrent, color='red', linestyle='--', linewidth=1.5,
               alpha=0.7, label=f'Maximum: {max_concurrent} jobs')

    # Add horizontal line for mean
    ax.axhline(mean_concurrent, color='blue', linestyle=':', linewidth=1.5,
               alpha=0.7, label=f'Mean: {mean_concurrent:.1f} jobs')

    # Labels and title
    ax.set_xlabel('Elapsed Time (hours)', fontsize=12)
    ax.set_ylabel('Number of Concurrent Jobs', fontsize=12)
    ax.set_title(f'HTCondor Concurrent Jobs Over Time (resolution: {resolution_seconds}s)',
                 fontsize=14, fontweight='bold')

    # Legend
    ax.legend(loc='upper right', fontsize=10)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')

    # Set y-axis to start at 0
    ax.set_ylim(bottom=0)

    # Add statistics text box
    stats_text = (
        f"Max: {max_concurrent} jobs\n"
        f"Mean: {mean_concurrent:.1f} jobs\n"
        f"Median: {median_concurrent:.1f} jobs\n"
        f"Total jobs: {len(df_valid)}"
    )
    ax.text(0.02, 0.98, stats_text,
            transform=ax.transAxes,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=10)

    # Tight layout
    plt.tight_layout()

    # Save figure as PDF
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")

    # Show plot if requested
    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description='Plot maximum concurrent jobs as a function of time'
    )
    parser.add_argument(
        'parquet_file',
        type=str,
        help='Path to HTCondor jobs parquet file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='concurrent_jobs.pdf',
        help='Output PDF file path (default: concurrent_jobs.pdf)'
    )
    parser.add_argument(
        '--resolution',
        type=int,
        default=30,
        help='Time resolution in seconds (default: 30)'
    )
    parser.add_argument(
        '--plot',
        action='store_true',
        help='Show interactive plot (default: save only)'
    )

    args = parser.parse_args()

    # Validate input file
    parquet_path = Path(args.parquet_file)
    if not parquet_path.exists():
        print(f"Error: File not found: {args.parquet_file}")
        return 1

    # Load parquet file
    print(f"Loading {args.parquet_file}...")
    df = pd.read_parquet(args.parquet_file)

    if len(df) == 0:
        print("Error: No data found in parquet file")
        return 1

    # Validate required columns
    required_cols = ['job_start_time', 'completion_time']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return 1

    # Create plot
    plot_concurrent_jobs(df, args.output, args.resolution, args.plot)

    return 0


if __name__ == '__main__':
    exit(main())
