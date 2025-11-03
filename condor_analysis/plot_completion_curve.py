#!/usr/bin/env python3
"""
Plot job completion curve from HTCondor job history parquet file.

This script creates a visualization showing the cumulative number of jobs
completed over time.

Usage:
    python plot_completion_curve.py <parquet_file> [--output OUTPUT_PNG]

Example:
    python plot_completion_curve.py condor_jobs.parquet --output completion_curve.png
"""

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def plot_completion_curve(df, output_file='completion_curve.png', show_plot=False):
    """
    Create a plot showing cumulative job completions over time.

    Args:
        df: pandas.DataFrame with completion_time column
        output_file: Path to output image file
    """
    print(f"Creating completion curve plot...")

    # Filter out jobs without completion time
    df_complete = df[df['completion_time'].notna()].copy()

    print(f"Total jobs: {len(df)}")
    print(f"Jobs with completion_time: {len(df_complete)}")

    # Sort by completion time
    df_complete = df_complete.sort_values('completion_time')

    # Create cumulative count
    df_complete['cumulative_count'] = range(1, len(df_complete) + 1)

    # Calculate statistics
    first_completion = df_complete['completion_time'].min()
    last_completion = df_complete['completion_time'].max()
    total_duration = (last_completion - first_completion).total_seconds()

    # Calculate relative duration in hours from first completion
    df_complete['relative_duration_hours'] = (
        (df_complete['completion_time'] - first_completion).dt.total_seconds() / 3600
    )

    print(f"\nCompletion Statistics:")
    print(f"  First completion: {first_completion}")
    print(f"  Last completion:  {last_completion}")
    print(f"  Total duration:   {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
    print(f"  Completion rate:  {len(df_complete) / (total_duration/60):.1f} jobs/minute")

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot completion curve
    ax.plot(df_complete['relative_duration_hours'], df_complete['cumulative_count'],
            linewidth=2, color='#2ca02c', label='Job Completions')

    # Labels and title
    ax.set_xlabel('Elapsed Time (hours)', fontsize=12)
    ax.set_ylabel('Cumulative Jobs Completed', fontsize=12)
    ax.set_title('HTCondor Job Completion Curve', fontsize=14, fontweight='bold')

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')

    # Add statistics text box
    stats_text = (
        f"Total Jobs: {len(df_complete)}\n"
        f"Duration: {total_duration/60:.1f} min\n"
        f"Rate: {len(df_complete) / (total_duration/60):.1f} jobs/min"
    )
    ax.text(0.02, 0.98, stats_text,
            transform=ax.transAxes,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=10)

    # Tight layout
    plt.tight_layout()

    # Save figure
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")

    # Show plot if requested
    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description='Plot job completion curve from HTCondor job history parquet file'
    )
    parser.add_argument(
        'parquet_file',
        type=str,
        help='Path to HTCondor jobs parquet file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='completion_curve.png',
        help='Output image file path (default: completion_curve.png)'
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

    if 'completion_time' not in df.columns:
        print("Error: 'completion_time' column not found in DataFrame")
        print(f"Available columns: {list(df.columns)}")
        return 1

    # Create plot
    plot_completion_curve(df, args.output, args.plot)

    return 0


if __name__ == '__main__':
    exit(main())
